#!/usr/bin/env python
"""
TODO:
- Add verification of content (MD5 local, upload manifest)
- Add a --list-backup-points command
- Add a --manifest --backup=<backup point> command
- Add a --restore --restore-from-datacenter= --backup=<backup point> command
- Add a --nagios-check
  - Connect to AWS S3 Grab the latest manifest from a host in the current data center
  - Find the most recent valid restore point - Confirm it is within a specified range
"""

__author__ = "Jose Avila III"
__copyright__ = "Copyright 2019, ONZRA LLC"
__credits__ = ["Jose Avila"]
__maintainer__ = "Jose Avila III"
__email__ = "javila@onzra.com"

import abc
import argparse
import ConfigParser
import fcntl
import glob
import json
import logging
import os
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
import yaml

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CASSANDRA_CONFIG_FILE = '/etc/cassandra/conf/cassandra.yaml'
DRY_RUN = False
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
RESTORE_DIR_FILELOCK = None


def glob_optimize_backup_paths(backup_paths):
    """
    Optimize a list of backup file paths to use a wildcard * character.

    Example of input:
        /tmp/ks/cf/backups/lb-1-big-Index.db
        /tmp/ks/cf/backups/lb-2-big-Summary.db
        /tmp/ks2/cf2/backups/lb-3-big-Index.db
        /tmp/ks2/cf2/backups/lb-3-big-Summary.db
    Output:
        /tmp/ks/cf/backups/lb-1-*
        /tmp/ks/cf/backups/lb-2-*
        /tmp/ks2/cf2/backups/lb-3-*

    :param list[str] backup_paths: list of file paths.

    :rtype: list[str]
    :return: reduced list of paths with wildcard * character replacements.
    """
    output = []
    for backup_path in backup_paths:
        base_path = '{0}/backups/'.format(backup_path.split('/backups/')[0])
        wildcard_path = '{0}{1}-*'.format(base_path, '-'.join(backup_path.split('/backups/')[1].split('-')[0:2]))
        output.append(wildcard_path)

    output = list(set(output))

    return output


def filter_keyspaces(keyspaces, columnfamily):
    """
    Filter provided keyspaces by dot separated keyspace and columnfamily string: '<KEYSPACE>.<COLUMNFAMILY>'

    :param dict keyspaces: keyspaces.
    :param str columnfamily: dot separated keyspace and columnfamily.

    :rtype: dict
    :return: filtered dict of keyspaces.
    """
    keyspace, columnfamily = columnfamily.split('.')
    # Filter by keyspace.
    filtered_keyspace = keyspaces[keyspace]
    # Filter by column family.
    filtered_keyspace['tables'] = {columnfamily: filtered_keyspace['tables'][columnfamily]}
    # Set keyspaces to only have filtered keyspace and columnfamily.
    keyspaces = {keyspace: filtered_keyspace}

    return keyspaces


def filename_strip(filename):
    """
    Remove characters from provided filename that may be occasionally problematic such as spaces.

    :param str filename: filename from which to remove characters.

    :rtype: str
    :return: stripped string.
    """
    return filename.replace(' ', '_').replace(':', '-')


def to_human_readable_time(seconds=None):
    """
    Return a human readable date time string from provided seconds or current time if not provided.

    :param int seconds: optional seconds since epoch.

    :rtype: str
    :return: human readable date time string.
    """
    return time.strftime(TIME_FORMAT, time.gmtime(seconds))


def from_human_readable_time(datetime):
    """
    Convert provided datetime string to seconds from Epoch.

    :param str datetime: date time string matching TIME_FORMAT format.

    :rtype: int
    :return: integer of seconds since Epoch.
    """
    return int(time.mktime(time.strptime(datetime, TIME_FORMAT)))


class FileLockedError(Exception):
    """
    Error raised when lock file exists.
    """
    pass


def filelocked(lockfile_path_or_lambda):
    """
    Decorator for a class method to check if provided lockfile_path should stop execution of decorated function.

    :param str lockfile_path_or_lambda: path to lock file or lambda which returns path.
    """
    def real_decorator(function):
        def wrapper(self, *args, **kwargs):
            if isinstance(lockfile_path_or_lambda, type(lambda: None)):
                lockfile_path = lockfile_path_or_lambda()
            else:
                lockfile_path = lockfile_path_or_lambda

            with open(lockfile_path, 'w') as f:
                try:
                    fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except IOError:
                    raise FileLockedError(lockfile_path)

                function(self, *args, **kwargs)

                if os.path.isfile(lockfile_path):
                    os.remove(lockfile_path)

        return wrapper

    return real_decorator


def run_command(cmd, execute_during_dry_run=False):
    """
    Run command.

    :param list cmd: Command to execute.
    :param bool execute_during_dry_run: execute this command when script is in dry run mode (--dry-run).

    :rtype: tuple
    :return: (return code, stdout, stderr)
    """
    global DRY_RUN

    sanitized_cmd = list(cmd)
    if 'cqlsh' in cmd and '-p' in cmd:
        pindex = cmd.index('-p')
        sanitized_cmd[pindex + 1] = '********'

    if DRY_RUN:
        logging.info('$ {0}'.format(' '.join(sanitized_cmd)))
        if not execute_during_dry_run:
            return 0, '', ''

    logging.debug('Run command: {0}'.format(sanitized_cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()

    if p.returncode != 0:
        # AWS sync and copy commands may return exit code 2 with the following message:
        # "warning: Skipping file <PATH>. File is character special device, block special device, FIFO, or socket."
        # This is an outstanding old bug with no updates: https://github.com/aws/aws-cli/issues/1117
        if p.returncode == 2 and (cmd[0] == 'aws' and cmd[1] == 's3' and cmd[2] in ('sync', 'cp')):
            logger.warn('Command {0} exited with code {1}. STDERR: "{2}"'.format(' '.join(cmd), p.returncode, err))
        else:
            raise Exception('Command {0} exited with code {1}. STDERR: "{2}"'.format(
                ' '.join(sanitized_cmd), p.returncode, err))
    logging.debug('Return code: {0}'.format(p.returncode))
    if out:
        if '\n' in out.strip():
            logging.debug('Output: \n{0}'.format(out))
        else:
            logging.debug('Output: {0}'.format(out.strip()))
    if err:
        logging.debug('Error: {0}'.format(err))
    return p.returncode, out, err


def get_version():
    """
    Get Cassandra version through CQLSH.

    :rtype: (str, str, str)
    :return: major, minor, patch.
    """
    cmd = ['cqlsh', '-e', 'SHOW VERSION',]
    append_cqlsh_args(cmd, args)

    return_code, out, error = run_command(cmd, execute_during_dry_run=True)
    major, minor, patch = out.split('|')[1].strip().split(' ')[1].split('.')
    return major, minor, patch


def append_cqlsh_args(cmd, args):
    """
    Update arguments list for CQLSH command.

    :param list cmd: command arguments list to update.
    :param dict args: ArgParser dict.
    """
    if args.cqlsh_host:
        cmd.append(args.cqlsh_host)
    if args.cqlsh_ssl:
        cmd.append('--ssl')
    if args.cqlsh_user:
        cmd.append('-u')
        cmd.append(args.cqlsh_user)
    if args.cqlsh_user:
        cmd.append('-p')
        cmd.append(args.cqlsh_pass)


class BaseBackupRepo(object):
    """
    Base backup repository class.
    """
    args = None
    store_md5sum = False

    def __init__(self, meta_path, store_md5sum=False):
        """
        Init.

        :param str meta_path: meta path.
        :param bool store_md5sum: optionally instruct backup repository to store MD5 checksum for stored files.
        """
        self.meta_path = meta_path
        self.store_md5sum = store_md5sum

    @abc.abstractmethod
    def download_manifests(self, host_id, columnfamily):
        """
        Download all host manifests for provided host id from remote storage. Optionally only download manifests for
        provided columnfamily.
        """
        pass

    @abc.abstractmethod
    def upload_manifests(self, host_id, ks_cf=None, manifest_files=None):
        """
        Upload all host manifests for provided host id to remote storage. Optionally provide list of paths to upload.

        :param str host_id: host id.
        :param list[str] manifest_files: if this optional argument is None, upload all manifests for the keyspaces and
        columnfamilies in this host. If the list contains paths, this function will only upload the provided paths.
        """
        pass

    @abc.abstractmethod
    def upload_snapshot(self, host_id, data_file_directories, snapshot_name):
        """
        Upload snapshot to remote storage by iterating through provided list of data_file_directories.

        :param str host_id: host id.
        :param list[str] data_file_directories: list of data file directories.
        :param str snapshot_name: name of snapshot.
        """
        pass

    @abc.abstractmethod
    def upload_incremental_backups(self, host_id, data_file_directory, filepath, incremental_directories):
        """
        Upload incremental backups to S3 for all incremental_directories in provided provided data_file_directory.

        :param str host_id: host id.
        :param str data_file_directory: data file directory.
        :param list[str] incremental_directories: list of incremental directories.
        """
        pass

    @abc.abstractmethod
    def upload_host_list(self, host_list_file_path):
        """
        Upload host list file to remote storage.

        :param str host_list_file_path: path to host list file to upload to remote storage.
        """
        pass

    @abc.abstractmethod
    def list_host_lists(self):
        """
        Return list of all host lists available in remote storage.

        :rtype: list[str]
        :return: list of host list strings.
        """
        pass

    @abc.abstractmethod
    def download_host_list(self, host_id, timestamp):
        """
        Download host list for provided host id and timestamp from remote storage and return local path to file.

        :param str host_id: host id.
        :param str timestamp: timestamp.

        :rtype: str
        :return: path to local file.
        """
        pass

    @abc.abstractmethod
    def list_columnfamilies_in_keyspace(self, host_id, keyspace):
        """
        List columnfamilies in provided keyspace.

        :param str host_id: host id.
        :param str keyspace: keyspace.

        :rtype: list[str]
        :return: list of columnfamily strings.
        """
        pass

    @abc.abstractmethod
    def list_snapshot_files(self, host_id, keyspace, columnfamily, snapshot_name):
        """
        List snapshot files for provided host id, keyspace, columnfamily, and snapshot name in remote storage.

        :param str host_id: host id.
        :param str keyspace: keyspace.
        :param str columnfamily: columnfamily.
        :param str snapshot_name: snapshot name.

        :rtype: list[str]
        :return: list of files.
        """
        pass

    @abc.abstractmethod
    def list_backup_files(self, host_id, keyspace, columnfamily):
        """
        List backup files for provided host id, keyspace, and columnfamily in remote storage.

        :param str host_id: host id.
        :param str keyspace: keyspace.
        :param str columnfamily: columnfamily.

        :rtype: list[str]
        :return: list of files.
        """
        pass

    @abc.abstractmethod
    def sync_files(self, remote_files, local_path):
        """
        Download provided list of remote_files from remote storage to provided local_path.

        :param list[str] remote_files: list of full paths to remote files.
        :param str local_path: local path where to store downloaded files.
        """
        pass


class AWSBackupRepo(BaseBackupRepo):
    """
    Use S3 to store meta data and backup files on Amazon. Files are organized in an S3 bucket using the host id. A meta
    folder is created to store snapshots of the cluster state.
    """

    @classmethod
    def build_parser(cls, sub_parser):
        parser = action_subparser.add_parser('aws', help='AWS help')
        parser.set_defaults(repo=cls)
        parser.add_argument('--aws-s3-bucket', dest='s3_bucket', default=None, required=True,
                            help='The AWS S3 Directory to upload this backup to.')
        parser.add_argument('--aws-s3-metadata-bucket', dest='s3_metadata_bucket', default=None, required=True,
                            help='The AWS S3 Directory to upload this backup metadata to.')
        parser.add_argument('--aws-s3-sse', dest='s3_sse', default=True,
                            help='Use SSE for the connection to S3')
        parser.add_argument('--aws-s3-storage-class', dest='s3_storage_class', required=False,
                            help='Optionally provide storage class for S3', default='STANDARD_IA')
        return parser

    def __init__(self, meta_path, s3_bucket, s3_metadata_bucket, s3_storage_class, s3_ss3):
        """
        Init.

        :param str meta_path: meta path.
        :param str s3_bucket: S3 bucket.
        :param str s3_metadata_bucket: S3 metadata bucket.
        :param str s3_storage_class: S3 storage class.
        :param bool s3_ss3: S3 server side encryption flag.
        """
        super(AWSBackupRepo, self).__init__(meta_path)

        # Trim the trailing slash
        if s3_bucket.endswith('/'):
            s3_bucket = s3_bucket[:-1]
        if s3_metadata_bucket.endswith('/'):
            s3_metadata_bucket = s3_metadata_bucket[:-1]

        self.s3_bucket = s3_bucket
        self.s3_metadata_bucket = s3_metadata_bucket
        self.s3_sse = s3_ss3
        self.s3_storage_class = s3_storage_class

    def upload_snapshot(self, host_id, data_file_directories, snapshot_name, thread_limit=4):
        """
        Upload snapshot to remote storage by iterating through provided list of data_file_directories.

        :param str host_id: host id.
        :param list[str] data_file_directories: list of data file directories.
        :param str snapshot_name: name of snapshot.
        """
        commands_to_run = []
        for data_file_directory in data_file_directories:
            bucket = '{0}/{1}'.format(self.s3_bucket, host_id)

            logging.info('Uploading full backup {0} dir to bucket: {1}'.format(data_file_directory, bucket))

            for path in glob.glob('{0}/*/*/snapshots/{1}/'.format(data_file_directory, snapshot_name)):
                remote_path = '{0}/{1}'.format(bucket, path.replace(data_file_directory, ''))
                cmd = ['aws', 's3', 'cp', '--recursive']
                if self.s3_storage_class:
                    cmd += ['--storage-class', self.s3_storage_class]
                cmd += [path, remote_path]
                if self.s3_sse:
                    cmd.append('--sse')

                commands_to_run.append(cmd)

        logging.info('Preparing to upload {0} files using {1} threads.'.format(len(commands_to_run), thread_limit))
        for ti in range(0, len(commands_to_run), thread_limit):
            commands_to_run_subset = commands_to_run[ti:ti + thread_limit]

            logging.info('Starting {0} threads.'.format(len(commands_to_run_subset)))
            upload_threads = []
            for command_to_run in commands_to_run_subset:
                upload_thread = threading.Thread(target=run_command, args=(command_to_run,))
                upload_threads.append(upload_thread)
                upload_thread.start()

            for upload_thread in upload_threads:
                upload_thread.join()

    def upload_incremental_backups(self, host_id, data_file_directory, filepath=None, columnfamily=None):
        """
        Upload incremental backups to S3 for all incremental_directories in provided provided data_file_directory.

        :param str host_id: host id.
        :param str data_file_directory: data file directory.
        :param list[str] incremental_directories: list of incremental directories.
        """
        cmd = ['aws', 's3', 'sync', data_file_directory]
        bucket = '{0}/{1}'.format(self.s3_bucket, host_id)
        if filepath:
            logging.info('Uploading incremental backup {0} to bucket: {1}'.format(filepath, bucket))
        else:
            logging.info('Uploading incremental backup to bucket: {0}'.format(bucket))
        cmd.append(bucket)
        cmd.extend(['--exclude', '*'])
        #  Upload all column families backups
        if filepath:
            local_path = '{0}{1}'.format(data_file_directory, filepath)
            remote_path = '{0}/{1}'.format(bucket, filepath)
            if local_path.endswith('/'):
                cmd = ['aws', 's3', 'cp']
                if self.s3_storage_class:
                    cmd += ['--storage-class', self.s3_storage_class]
                cmd += ['--recursive', local_path, remote_path]
                cmd.extend(['--exclude', '{0}/.*'.format(local_path)])
            else:
                cmd = ['aws', 's3', 'cp']
                if self.s3_storage_class:
                    cmd += ['--storage-class', self.s3_storage_class]
                cmd += [local_path, remote_path]

                cmd.extend(['--exclude', '{0}/.*'.format(local_path)])
        else:
            if columnfamily:
                ks, cf = columnfamily.split('.')
                cmd.extend(['--include', '{0}{1}/{2}*/backups/*'.format(data_file_directory, ks, cf)])
                cmd.extend(['--exclude', '{0}{1}/{2}*/backups/*/.*'.format(data_file_directory, ks, cf)])
            else:
                cmd.extend(['--include', '{0}*/*/backups/*'.format(data_file_directory)])
                cmd.extend(['--exclude', '{0}*/*/backups/*/.*'.format(data_file_directory)])

        if self.s3_sse:
            cmd.append('--sse')

        run_command(cmd)

    def download_manifests(self, host_id, columnfamily=None):
        """
        Download all host manifests for provided host id from remote storage. Optionally only download manifests for
        provided columnfamily.

        :param str host_id: host id.
        :param str columnfamily: optional columnfamily specification.
        """
        local_path = '{0}/'.format(self.meta_path)
        s3_path = '{0}/{1}'.format(self.s3_metadata_bucket, host_id)

        if not columnfamily:
            logging.info('Downloading manifest files to: {0}'.format(local_path))
        else:
            logging.info('Downloading manifest files for {cf} to: {lp}'.format(cf=columnfamily, lp=local_path))

        cmd = []
        if columnfamily is not None:
            ks, cf = columnfamily.split('.')
            manifest = '{0}/{1}/meta/manifest.json'.format(ks, cf)
            s3_path = '{0}/{1}'.format(s3_path, manifest)
            local_path = '{0}/{1}'.format(local_path, manifest)
            cmd.extend(['aws', 's3', 'cp', s3_path, local_path])
        else:
            cmd.extend(['aws', 's3', 'cp', '--recursive', s3_path, local_path])
            cmd.extend(['--exclude', '*', '--include', '*/*/meta/manifest.json'])

        if self.s3_sse:
            cmd.append('--sse')

        run_command(cmd)

    def upload_manifests(self, host_id, columnfamily=None, manifest_files=None):
        """
        Upload all host manifests to remote storage. Optionally provide list of paths to upload.

        :param str host_id: host id.
        :param list[str] manifest_files: if this optional argument is None, upload all manifests for the keyspaces and
        columnfamilies in this host. If the list contains paths, this function will only upload the provided paths.
        """
        local_path = '{0}/'.format(self.meta_path)
        s3_path = '{0}/{1}'.format(self.s3_metadata_bucket, host_id)

        if not columnfamily:
            logging.info('Uploading manifest files to: {0}'.format(local_path))
        else:
            logging.info('Uploading manifest files for {cf} to: {lp}'.format(cf=columnfamily, lp=local_path))

        cmd = []

        if columnfamily is not None:
            ks, cf = columnfamily.split('.')
            manifest = '{0}/{1}/meta/manifest.json'.format(ks, cf)
            s3_path = '{0}/{1}'.format(s3_path, manifest)
            local_path = '{0}/{1}'.format(local_path, manifest)
            cmd.extend(['aws', 's3', 'cp', local_path, s3_path])
        else:
            cmd.extend(['aws', 's3', 'cp', '--recursive', local_path, s3_path])
            cmd.extend(['--exclude', '*', '--include', '*/*/meta/manifest.json'])


        if self.s3_sse:
            cmd.append('--sse')

        run_command(cmd)

    def upload_host_list(self, host_list_file_path):
        """
        Upload host list file to remote storage.
        """
        local_path = host_list_file_path
        filename = os.path.basename(local_path)
        cmd = []

        cmd.extend(['aws', 's3', 'cp'])
        bucket = '{0}/meta/{1}'.format(self.s3_metadata_bucket, filename)
        cmd.extend([local_path, bucket])

        if self.s3_sse:
            cmd.append('--sse')
        run_command(cmd)

    def list_columnfamilies_in_keyspace(self, host_id, keyspace):
        """
        List columnfamilies in provided keyspace.

        :param str host_id: host id.
        :param str keyspace: keyspace.

        :rtype: list[str]
        :return: list of columnfamily strings.
        """
        path = '{0}/{1}/{2}/'.format(self.s3_bucket, host_id, keyspace)
        cmd = ['aws', 's3', 'ls', path]
        _, out, _ = run_command(cmd)
        return [o.split('PRE ')[1] for o in out.split('\n') if 'PRE ' in o]

    def list_host_lists(self):
        """
        Return list of all host lists available in remote storage.

        :rtype: list[str]
        :return: list of host list strings.
        """
        path = '{0}/meta/'.format(self.s3_metadata_bucket)
        cmd = ['aws', 's3', 'ls', path]
        _, out, _ = run_command(cmd)
        return [o.split(' ')[-1] for o in out.split('\n') if '_' in o]

    def download_host_list(self, host_id, timestamp):
        """
        Download host list for provided host id and timestamp from remote storage and return local path to file.

        :param str host_id: host id.
        :param str timestamp: timestamp.

        :rtype: str
        :return: path to local file.
        """
        filename = '{0}_{1}.json'.format(host_id, timestamp)

        remote_path = '{0}/meta/{1}'.format(self.s3_metadata_bucket, filename)
        local_path = '{mp}/{fn}'.format(mp=self.meta_path, fn=filename)

        cmd = ['aws', 's3', 'cp', remote_path, local_path]
        run_command(cmd)
        return local_path

    def list_snapshot_files(self, host_id, keyspace, columnfamily, snapshot_name):
        """
        List snapshot files for provided host id, keyspace, columnfamily, and snapshot name in remote storage.

        :param str host_id: host id.
        :param str keyspace: keyspace.
        :param str columnfamily: columnfamily.
        :param str snapshot_name: snapshot name.

        :rtype: list[str]
        :return: list of files.
        """
        path = '{0}/{1}/{2}/{3}/snapshots/{4}/'.format(self.s3_bucket, host_id, keyspace, columnfamily, snapshot_name)
        cmd = ['aws', 's3', 'ls', path]
        try:
            _, out, _ = run_command(cmd)
        except Exception as exception:
            if ' exited with code 1.' in exception.message:
                return []
        return [f.split(' ')[-1] for f in out.strip().split('\n')]

    def list_backup_files(self, host_id, keyspace, columnfamily):
        """
        List backup files for provided host id, keyspace, and columnfamily in remote storage.

        :param str host_id: host id.
        :param str keyspace: keyspace.
        :param str columnfamily: columnfamily.

        :rtype: list[str]
        :return: list of files.
        """
        path = '{0}/{1}/{2}/{3}/backups/'.format(self.s3_bucket, host_id, keyspace, columnfamily)
        cmd = ['aws', 's3', 'ls', path]
        try:
            _, out, _ = run_command(cmd)
        except Exception as exception:
            if ' exited with code 1.' in exception.message:
                return []

        return [f.split(' ')[-1] for f in out.strip().split('\n')]

    def sync_files(self, remote_files, local_path, base_path=None):
        """
        Download provided list of remote_files from remote storage to provided local_path.

        :param list[str] remote_files: list of full paths to remote files.
        :param str local_path: local path where to store downloaded files.
        """
        if base_path is None:
            cmd = ['aws', 's3', 'cp', '--recursive', self.s3_bucket, local_path]
        else:
            base_path = '{0}/{1}'.format(self.s3_bucket, base_path)
            cmd = ['aws', 's3', 'cp', '--recursive', base_path, local_path]

        # logging.info('Downloading {0} files from {1} to {2}.'.format(len(remote_files), self.s3_bucket, local_path))
        cmd.extend(['--exclude', '*'])
        #  Upload all column families backups
        for remote_file in remote_files:
            cmd.extend(['--include', remote_file])
        if self.s3_sse:
            cmd.append('--sse')

        run_command(cmd)

    def download_files(self, remote_path, local_path):
        """
        Recursively download a directory from remote storage to provided local_path.

        :param str remote_path: list of full paths to remote files.
        :param str local_path: local path where to store downloaded files.
        """
        remote_path = '{0}/{1}'.format(self.s3_bucket, remote_path)

        if '*' in remote_path:
            cmd = ['aws', 's3', 'cp', '--recursive', '/'.join(remote_path.split('/')[0:-1])+'/', local_path]
            cmd.extend(['--exclude', '*'])
            include_path = remote_path.split('/')[-1]
            cmd.extend(['--include', include_path])
        else:
            cmd = ['aws', 's3', 'cp', '--recursive', remote_path, local_path]

        if self.s3_sse:
            cmd.append('--sse')

        run_command(cmd)


class Cassandra(object):
    """
    The purpose of this class is to discover information about the host and cluster through nodetool commands, cqlsh
    describe schema commands, and the cassandra config file.
    """
    cqlsh_host = None
    config_file = None

    meta_path = None

    nodetool_info_data = {}
    keyspace_schema_data = {}
    cluster_data = {}

    keyspace_columnfamily_filter = None

    config = None

    @property
    def host_id(self):
        if 'ID' not in self.nodetool_info_data:
            self.__enumerate_info()
        return self.nodetool_info_data['ID']

    @property
    def data_file_directories(self):
        if not self.config:
            self.__enumerate_config()

        data_file_directories = self.config['data_file_directories']

        # Standardize data file directory paths to end in a trailing slash.
        data_file_directories = ['{0}/'.format(dfd.rstrip('/')) for dfd in data_file_directories]

        return data_file_directories

    @property
    def host_info(self):
        if not self.nodetool_info_data:
            self.__enumerate_info()
        return self.nodetool_info_data

    @property
    def schema_info(self):
        if not self.keyspace_schema_data:
            self.__enumerate_keyspaces()
        return self.keyspace_schema_data

    @property
    def cluster_info(self):
        if not self.cluster_data:
            self.__enumerate_cluster()
        return self.cluster_data

    @property
    def backupenabled(self):
        """
        Check if cassandra is configured for incremental backups.

        :rtype: bool
        :return: True if incremental backups are enabled.
        """
        return self.nodetool_statusbackup() == 'running'

    def __init__(self, args):
        """
        Init.

        :param Namespace args: args.
        """
        self.config_file = args.cassandra_config
        # TODO: This isn't used?
        # self.cqlsh_host = args.cqlsh_host

        if 'columnfamily' in args:
            self.keyspace_columnfamily_filter = args.columnfamily

        self.set_meta_path(args.meta_path)

    def set_meta_path(self, meta_path):
        """
        Set the path where meta files for backup services will be stored. The path selected is based off of the first
        Cassandra data file directory setting.

        :param str meta_path: path for which to store meta data json files.
        """
        logging.debug('Setting meta path: {0}'.format(meta_path))
        if not os.path.exists(meta_path):
            os.makedirs(meta_path)
        self.meta_path = meta_path

    def __enumerate_config(self):
        """
        Load Cassandra config file.
        """
        # Load configuration
        try:
            f = open(self.config_file, 'r')
            cassandra_configfile = f.read()
            f.close()
            self.config = yaml.load(cassandra_configfile, Loader=yaml.FullLoader)
        except Exception as exception:
            logging.critical('Could not load cassandra config file from {0}. Error: {1}'.format(
                CASSANDRA_CONFIG_FILE, str(exception)))
            raise

    def __enumerate_cluster(self):
        """
        Populate the cluster_data dict with state, address, and rack values per datacenter.

        :rtype: dict
        :return: Dictionary of hosts: [column families]
        """
        return_code, out, error = run_command(['nodetool', 'status'], execute_during_dry_run=True)
        datacenter = None
        skiplines = [
            'Status=Up/Down',
            '|/ State=Normal/Leaving/Joining/Moving',
            '',
        ]
        for line in out.split("\n"):
            if line in skiplines or line.startswith('-- ') or line.startswith('===') or line.startswith('Note: '):
                continue
            if line.startswith('Datacenter: '):
                datacenter = line.split('Datacenter: ')[1]
                self.cluster_data[datacenter] = {}
            else:
                # When a node is down the load quantity is "?" and load units is missing.
                if '?' in line and len(line.split()) == 7:
                    line = line.replace('?', '? ?')

                status_state, address, load_qty, load_units, tokens, owns, host_id, rack = line.split()
                status = status_state[0]
                self.cluster_data[datacenter][host_id] = {
                    'state': status_state[1],
                    'address': address,
                    'rack': rack,
                }

    def __enumerate_info(self):
        """
        Provide information about the host such as host_id

        :rtype: dict
        :return: Dictionary of hosts: [column families]
        """
        cmd = ['nodetool', 'info']
        return_code, out, error = run_command(cmd, execute_during_dry_run=True)
        for line in out.split("\n"):
            if not line:
                continue
            key, value = line.split(":")
            self.nodetool_info_data[key.strip()] = value.strip()

    def __enumerate_keyspaces(self):
        """
        Get a dict of all keyspaces and their column families.

        :rtype: dict
        :return: Dictionary of keyspace: [column families]
        """
        # TODO: nodetool cfstats is replaced with nodetool tablestats in 2.2. cfstats exists as a deprecated reference.
        cmd = ['nodetool', 'cfstats']
        # TODO: Fix this. Cannot filter here because it breaks status command.
        # if self.keyspace_columnfamily_filter is not None:
        #     cmd.append(self.keyspace_columnfamily_filter)

        return_code, out, error = run_command(cmd, execute_during_dry_run=True)
        # Build a dictionary of keyspace: [column families]
        keyspace = None

        line_start = 'Keyspace: '
        version = get_version()
        if version[0] == '3' and version[1] != '0':
            line_start = 'Keyspace : '

        for line in out.split("\n"):
            if line.startswith(line_start):
                keyspace = line.split(line_start)[1]

                self.keyspace_schema_data[keyspace] = self.__enumerate_keyspace_replication(keyspace)
                self.keyspace_schema_data[keyspace]['tables'] = []
            elif line.startswith("\t\tTable: "):
                table = line.split('\t\tTable: ')[1]
                self.keyspace_schema_data[keyspace]['tables'].append(table)

    def __enumerate_keyspace_replication(self, keyspace):
        """
        Get a dictionary of options for the keyspace from cqlsh.

        :rtype: dict
        :return: Dictionary of replication options for the keyspace
        """
        cmd = [
            'cqlsh',
            '-e',
            'DESCRIBE KEYSPACE "{0}"'.format(keyspace),
        ]

        append_cqlsh_args(cmd, args)

        return_code, out, error = run_command(cmd, execute_during_dry_run=True)
        for line in out.split("\n"):
            if not line.startswith('CREATE KEYSPACE '):
                continue
            replication_str = line.split('replication = ')[1]
            replication_str = replication_str.split(' AND durable_writes')[0]
            replication_str = replication_str.replace('\'', '"').strip()
            return json.loads(replication_str)

    def columnfamily_id_map(self):
        """
        Return dict containing the columnfamily id which is used by Cassandra to prevent same name collisions on disk.

        Result format sample:
        {
            'system': {
                'peers': '37f71aca-...',
                'range_xfers': '55d76438-...'
            },
            'system_traces': {
                'events': '8826e8e9-...'
            }
        }

        :rtype: dict
        :return: dictionary of columnfamily id mapping for columnfamilies in keyspaces.
        """
        version = get_version()

        # Selecting JSON output was made available to CQL in Cassandra 2.2; earlier versions fail.
        if version[0] == '2' and version[1] == '1':
            cmd = [
                'cqlsh',
                '-e',
                'PAGING OFF; SELECT keyspace_name, columnfamily_name, cf_id FROM system.schema_columnfamilies LIMIT 1000000'
            ]
            append_cqlsh_args(cmd, args)
            _, out, _ = run_command(cmd)
            rows = []
            for row in out.split('\n')[4:-3]:
                keyspace_name, columnfamily_name, cf_id = row.split('|')
                rows.append({
                    'keyspace_name': keyspace_name.strip(),
                    'columnfamily_name': columnfamily_name.strip(),
                    'cf_id': cf_id.strip()
                })
        else:
            if version[0] == '2':
                cmd = [
                    'cqlsh',
                    '-e',
                    'PAGING OFF; SELECT JSON keyspace_name, columnfamily_name, cf_id FROM system.schema_columnfamilies LIMIT 1000000'
                ]
            elif version[0] == '3':
                cmd = [
                    'cqlsh',
                    '-e',
                    'PAGING OFF; SELECT JSON keyspace_name, table_name as columnfamily_name, id as cf_id FROM system_schema.tables LIMIT 1000000'
                ]
            append_cqlsh_args(cmd, args)
            _, out, _ = run_command(cmd)

            rows = [json.loads(r.strip()) for r in out.split('\n')[4:-3]]
        cf_id_map = {}
        for row in rows:
            keyspace = row['keyspace_name']
            columnfamily = row['columnfamily_name']
            cf_id = row['cf_id']

            if keyspace not in cf_id_map:
                cf_id_map[keyspace] = {}

            cf_id_map[keyspace][columnfamily] = cf_id

        return cf_id_map

    # Nodetool Commands
    def nodetool_flush(self):
        """
        Flush Cassandra memtable to SSTables on disk.
        """
        retcode, out, err = run_command(['nodetool', 'flush'])

    def nodetool_snapshot(self, snapshot_name, columnfamily=None):
        """
        Execute command which will cause Cassandra to flush the node before taking a snapshot, take the snapshot, and
        store the data in the snapshots directory of each keyspace in the data directory. If snapshot_name is not
        provided, then Cassandra will default to using a timestamp as the name (for example 1391460334889).

        :param str snapshot_name: optional string Cassandra will use to name the snapshot directory.
        :param str columnfamily: optionally limit to a specific columnfamily. Example: "keyspace.columnfamily"
        """
        cmd = ['nodetool', 'snapshot']
        if snapshot_name:
            cmd.extend(['-t', snapshot_name])

        if columnfamily is not None:
            cmd.extend(['-kt', '{0}'.format(columnfamily)])

        return_code, out, err = run_command(cmd)

    def nodetool_clearsnapshot(self, snapshot_name):
        """
        Deletes snapshots in one or more keyspaces. To remove all snapshots, omit the snapshot name.

        :param str snapshot_name: optional name of snapshot to delete.
        """
        cmd = ['nodetool', 'clearsnapshot']
        if snapshot_name:
            cmd.extend(['-t', snapshot_name])
        return_code, out, err = run_command(cmd)

    def nodetool_statusbackup(self):
        """
        Execute command requesting Cassandra's incremental backups state.

        :rtype: str
        :return: output from nodetool statusbackup command.
        """
        cmd = ['nodetool', 'statusbackup']
        return_code, out, err = run_command(cmd, execute_during_dry_run=True)
        return out.strip()

    def clear_incrementals(self, data_file_directory, incremental_files):
        """
        Delete all incremental_files in data_file_directory. This is recursive for non string entries (lists and tuples)
        in incremental_files - for example, ['file_1', ['file_2', 'file_3', ('file_4', 'file_5')]].

        :param str data_file_directory: path to Cassandra data file directory.
        :param list[str|tuple|list] incremental_files: list of strings, tuples, or lists containing path strings.
        """
        for incremental_file in incremental_files:
            if isinstance(incremental_file, str):
                # Don't delete the /backups/ directory as sstables may have been written during the upload operation.
                if incremental_file.endswith('/backups'):
                    continue
                # Don't delete the index files directory.
                if incremental_file.endswith('_idx/'):
                    continue
                path = os.path.join(data_file_directory, incremental_file)
                logging.info('Removing incremental path: {0}'.format(path))
                os.remove(path)
            else:
                self.clear_incrementals(data_file_directory, incremental_file)


class ManifestManager(object):
    """
    The ManifestManager handles updating and storing manifest files which describe the cluster and the state of backups.
    """

    cassandra = None
    meta_path = None
    backup_repo = None

    def __init__(self, cassandra, meta_path, backup_repo, retention_days=None):
        """
        Init.

        :param Cassandra cassandra: Cassandra information resource.
        :param str meta_path: meta path.
        :param BaseBackupRepo backup_repo: backup repository class.
        :param int|None retention_days: number of days to retain manifest data.
        """
        self.cassandra = cassandra
        self.meta_path = meta_path
        self.backup_repo = backup_repo
        self.retention_days = retention_days

    def get_md5sum(self, path):
        """
        Get MD5 checksum string for provided file path.

        :param str path: path to file for which to get MD5 checksum.

        :rtype: str
        :return: MD5 checksum string.
        """
        _, md5sum_result, _ = run_command(['md5sum', path])
        return md5sum_result.split(' ')[0].strip()

    def get_host_list_file_path(self):
        """
        Generate path to the host list file describing the Cassandra cluster's current state.

        :rtype: str
        :return: path to host list JSON file.
        """
        datetime_string = filename_strip(to_human_readable_time(int(time.time())))
        filename = '{0}_{1}.json'.format(self.cassandra.host_id, datetime_string)
        return '{mp}/{fn}'.format(mp=self.cassandra.meta_path, fn=filename)

    def get_manifest_file_path(self, keyspace, columnfamily):
        """
        Generate path to manifest file for provided keyspace and columnfamily.

        :param str keyspace: keyspace.
        :param str columnfamily: columnfamily.

        :rtype: str
        :return: path to keyspace columnfamily manifest JSON file.
        """
        return '{mp}/{ks}/{cf}/meta/manifest.json'.format(mp=self.meta_path, ks=keyspace, cf=columnfamily)

    def load_manifest(self, keyspace, columnfamily):
        """
        Load manifest file JSON for provided keyspace and columnfamily.

        :param str keyspace: keyspace.
        :param str columnfamily: columnfamily.

        :rtype: dict
        :return: manifest JSON dict.
        """
        manifest_file_path = self.get_manifest_file_path(keyspace, columnfamily)

        if os.path.exists(manifest_file_path):
            with open(manifest_file_path, 'r') as manifest_file:
                logging.info('Using local manifest file {0}'.format(manifest_file_path))
                try:
                    manifest = json.load(manifest_file)
                except ValueError as value_error:
                    logging.warning('Error loading manifest json: {0}'.format(value_error))
                    logging.warning('Creating new manifest to replace corrupted file - data will be added next backup.')
                    manifest = {
                        'keyspace': keyspace,
                        'column_family': columnfamily,
                        'created': to_human_readable_time()
                    }
        else:
            logging.info('Creating new manifest file {0}'.format(manifest_file_path))
            manifest = {
                'keyspace': keyspace,
                'column_family': columnfamily,
                'created': to_human_readable_time()
            }

        return manifest

    def update_snapshot_manifests(self, snapshot_name, columnfamily=None):
        """
        Insert snapshot data into manifest files using provided snapshot name.

        :param: str snapshot_name: snapshot name.
        :param str columnfamily: optionally perform full backup on only this keyspace and columnfamily.
        """
        if columnfamily is not None:
            logging.info('Updating full file list manifests for {0} for snapshot {1}.'.format(
                columnfamily, snapshot_name)
            )
        else:
            logging.info('Updating full file list manifests for all KS CF for snapshot {0}.'.format(snapshot_name))

        keyspaces = self.cassandra.schema_info

        if columnfamily is not None:
            keyspaces = filter_keyspaces(keyspaces, columnfamily)

        for ks in keyspaces:
            for cf in keyspaces[ks]['tables']:
                manifest = self.load_manifest(ks, cf)

                if 'full' not in manifest:
                    manifest['full'] = {}

                snapshot_manifest_data = {}

                for data_file_directory in self.cassandra.data_file_directories:
                    files = '{}{}/{}-*/snapshots/{}/*'.format(data_file_directory, ks, cf, snapshot_name)
                    glob_files = glob.glob(files)
                    for glob_filename in glob_files:
                        filename = os.path.basename(glob_filename)
                        if filename == 'manifest.json':
                            continue

                        snapshot_manifest_data[filename] = {
                            'created': to_human_readable_time(os.path.getmtime(glob_filename)),
                        }

                        if self.backup_repo.store_md5sum:
                            snapshot_manifest_data[filename]['md5sum'] = self.get_md5sum(glob_filename)

                manifest['full'][snapshot_name] = snapshot_manifest_data
                self.save_manifest(ks, cf, manifest)

    def save_host_list(self, data):
        """
        Save provided data to host list file and return path to saved data.

        :param dict data: JSON dictionary of host list data to save.

        :rtype: str
        :return: path to saved host list JSON file.
        """
        host_list_file_path = self.get_host_list_file_path()
        with open(host_list_file_path, 'w') as host_list_file:
            json.dump(data, host_list_file)

        return host_list_file_path

    def update_host_list(self):
        """
        Generate and upload a host list file. After uploading, remove the local host list file.
        """
        data = {
            'info': self.cassandra.host_info,
            'cluster': self.cassandra.cluster_info,
            'keyspaces': self.cassandra.schema_info,
        }

        # Update column families list to be a dict where the value is column family id.
        column_family_id_map = self.cassandra.columnfamily_id_map()
        for ks in data['keyspaces']:
            tables_keyed_by_cf_id = {}
            for cf in data['keyspaces'][ks]['tables']:
                tables_keyed_by_cf_id[cf] = column_family_id_map[ks][cf]
            data['keyspaces'][ks]['tables'] = tables_keyed_by_cf_id

        host_list_file_path = self.save_host_list(data)
        self.backup_repo.upload_host_list(host_list_file_path)
        try:
            os.remove(host_list_file_path)
        except OSError as os_error:
            logging.warning('OSError when removing host list file: {0}'.format(os_error))

    def get_host_lists(self):
        """
        Retrieve dict of latest host lists from remote storage keyed by host id.

        :rtype: dict
        :return: host data dictionary.
        """
        output = {}
        host_lists = self.backup_repo.list_host_lists()

        # Identify the latest host list file.
        latest_timestamp = 0
        host_id = None
        for hl in host_lists:
            host_list_timestamp = from_human_readable_time(
                '{} {}'.format(hl.split('_')[1], hl.split('_')[2].replace('-', ':')).replace('.json', '')
            )
            if host_list_timestamp > latest_timestamp:
                latest_timestamp = host_list_timestamp
                host_id = hl.split('_')[0]

        latest_timestamp_filename_string = filename_strip(to_human_readable_time(latest_timestamp))
        host_list_path = self.backup_repo.download_host_list(host_id, latest_timestamp_filename_string)

        # Download the latest host list file and extract host ids from cluster data.
        host_ids = []
        with open(host_list_path, 'r') as host_list_file:
            host_list_data = json.load(host_list_file)

            for dc in host_list_data['cluster']:
                host_ids += host_list_data['cluster'][dc].keys()

        host_ids = set(host_ids)

        # Get host data from latest host list for list of latest host ids.
        for host_id in host_ids:
            logging.debug('Getting latest timestamp for host: {0}'.format(host_id))
            logging.debug('{0}'.format(host_lists))
            host_lists_for_host_id = [hl for hl in host_lists if host_id in hl]
            logging.debug('Host lists with {0} entries filtered to {1} entries.'.format(len(host_lists),
                                                                                        len(host_lists_for_host_id)))
            host_list_timestamps = [
                from_human_readable_time(
                    '{} {}'.format(hl.split('_')[1], hl.split('_')[2].replace('-', ':')).replace('.json', '')
                ) for hl in host_lists_for_host_id
            ]
            try:
                latest_timestamp = max(host_list_timestamps)
                latest_timestamp_filename_string = filename_strip(to_human_readable_time(latest_timestamp))
                host_list_path = self.backup_repo.download_host_list(host_id, latest_timestamp_filename_string)
                with open(host_list_path, 'r') as host_list_file:
                    host_list_data = json.load(host_list_file)
                output[host_id] = host_list_data
            except ValueError as value_error:
                if host_id not in output:
                    output[host_id] = value_error

        return output

    def save_manifest(self, keyspace, columnfamily, manifest):
        """
        Save manifest file for provided keyspace and columnfamily.

        :param str keyspace: keyspace.
        :param str columnfamily: columnfamily.
        :param dict manifest: dict of manifest data to save.

        :rtype: str
        :return: path of manifest file that was saved.
        """
        if self.retention_days:
            self.manifest_data_retention_pruning(manifest)

        manifest['updated'] = to_human_readable_time()

        manifest_file_path = self.get_manifest_file_path(keyspace, columnfamily)

        if not os.path.exists(os.path.dirname(manifest_file_path)):
            os.makedirs(os.path.dirname(manifest_file_path))

        manifest_temp_file_path = '{0}.tmp'.format(manifest_file_path)
        with open(manifest_temp_file_path, 'w') as manifest_temp_file:
            json.dump(manifest, manifest_temp_file)

        os.rename(manifest_temp_file_path, manifest_file_path)

        return manifest_file_path

    def manifest_data_retention_pruning(self, data):
        """
        Prune provided data to only values within the defined self.retention_days time range.

        :param dict data: manifest data.
        """
        if 'full' in data:
            full_to_delete = []
            for full_ts in data['full']:
                full_days_old = int(time.mktime(time.gmtime()) - time.mktime(time.gmtime(int(full_ts)/1000)))/86400
                if full_days_old > self.retention_days:
                    full_to_delete.append(full_ts)

            for full_ts in full_to_delete:
                del data['full'][full_ts]
                logging.debug('Removing full record from manifest: {0}'.format(full_ts))

        if 'incremental' in data:
            inc_to_delete = []
            for inc_fn in data['incremental']:
                inc_ts = from_human_readable_time(data['incremental'][inc_fn]['created'])
                inc_days_old = int(time.mktime(time.gmtime()) - time.mktime(time.gmtime(inc_ts))) / 86400
                if inc_days_old > 30:
                    inc_to_delete.append(inc_fn)

            for inc_fn in inc_to_delete:
                del data['incremental'][inc_fn]
                logging.debug('Removing incremental record from manifest: {0}'.format(inc_fn))

    def download_manifests(self, host_id, columnfamily=None):
        """
        Download all manifest files from remotes storage.

        :param str host_id: host id.
        """
        self.backup_repo.download_manifests(host_id, columnfamily)

    def upload_manifests(self, host_id, ks_cf=None, manifest_files=None):
        """
        Upload all manifest files to remote storage.

        :param list[str] manifest_files: optional list of manifest files to filter for efficiency.
        """
        self.backup_repo.upload_manifests(host_id, ks_cf, manifest_files)

    def incremental_manifest(self, data_file_directory, incremental_files):
        """
        Generate, update, and store a manifest for incremental backups.

        :param str data_file_directory: data file directory for provided incremental_files.
        :param dict incremental_files: dict of keyed paths containing list of backup files for that path.

        :rtype: list[str]
        :return: list of updated manifest files.
        """
        manifests_updated = []

        logging.info('Updating incremental file list manifests...')
        for dir in incremental_files:
            # Skip adding index files to the manifest.
            if '/.' in dir and dir.endswith('_idx'):
                continue

            dir_split = dir.split('/')
            keyspace = dir_split[0]
            columnfamily = '-'.join(dir_split[1].split('-')[0:-1])

            manifest = self.load_manifest(keyspace, columnfamily)
            if 'incremental' not in manifest:
                manifest['incremental'] = {}

            for path in incremental_files[dir]:
                full_path = '{}{}'.format(data_file_directory, path)
                filename = path.replace(dir, '').strip('/')

                manifest['incremental'][filename] = {
                    'created': to_human_readable_time(os.path.getmtime(full_path)),
                }
                if self.backup_repo.store_md5sum:
                    manifest['incremental'][filename]['md5sum'] = self.get_md5sum(full_path)

            manifest_file_updated = self.save_manifest(keyspace, columnfamily, manifest)
            manifests_updated.append(manifest_file_updated)

        return manifests_updated


class BackupStatus(object):
    """
    BackupStatus.
    """

    def __init__(self, manifest_manager, backup_repo, restore_time=None, columnfamily=None, host_ids=None):
        """
        Init.

        :param ManifestManager manifest_manager: manifest manager.
        :param BaseBackupRepo backup_repo: remote storage object.
        :param int restore_time: timestamp for latest time which can be used to determine restore status and operations.
        :param str columnfamily: optional columnfamily specific target.
        :param list[str] host_ids: optionally filter by list of host ids.
        """
        self.manifest_manager = manifest_manager
        self.backup_repo = backup_repo
        self.host_statuses = {}
        if restore_time:
            self.restore_time = int(restore_time)
            logging.warning('Backup Status will find data available before this restore time: {0}'.format(
                to_human_readable_time(self.restore_time)
            ))
        else:
            self.restore_time = None

        host_lists = self.manifest_manager.get_host_lists()

        # TODO: Update the status function to move back in time to earlier manifests / host lists files.
        if all([type(host_lists[hl]) is ValueError for hl in host_lists]):
            logging.warn('Cannot determine latest timestamp for host: {0}'.format(','.join(host_lists.keys())))
            raise host_lists[host_lists.keys()[0]]

        host_lists = {hl: host_lists[hl] for hl in host_lists if type(host_lists[hl]) is dict}

        if host_ids is not None:
            host_lists = {hl: host_lists[hl] for hl in host_lists if hl in host_ids}

        logging.info('BackupStatus: Iterating through {0} hosts.'.format(len(host_lists)))
        for host_id in host_lists:
            if columnfamily is not None:
                self.manifest_manager.download_manifests(host_id, columnfamily=columnfamily)
            else:
                self.manifest_manager.download_manifests(host_id)
            host_status = self.add_host_status(host_id)

            keyspaces = host_lists[host_id]['keyspaces']
            if columnfamily is not None:
                keyspaces = filter_keyspaces(keyspaces, columnfamily)

            if columnfamily is not None:
                keyspaces = filter_keyspaces(keyspaces, columnfamily)

            logging.info('BackupStatus: Creating keyspace status containers for {0} keyspaces.'.format(len(keyspaces)))
            threads = {}
            for keyspace in keyspaces:
                threads[keyspace] = threading.Thread(target=host_status.add_keyspace_status, args=(keyspace,))
                threads[keyspace].start()

            for i in threads:
                threads[i].join()
            logging.info('BackupStatus: Keyspace containers complete.')

            threads = {}
            for keyspace in keyspaces:
                keyspace_status = host_status.keyspace_statuses[keyspace]

                # Populate columnfamilies in keyspace.
                for table in host_lists[host_id]['keyspaces'][keyspace]['tables']:

                    uuid_string = keyspaces[keyspace]['tables'][table].replace('-', '')
                    columnfamily_cfid = '{0}-{1}'.format(table, uuid_string)

                    threads[table] = threading.Thread(target=keyspace_status.add_columnfamily_status,
                                                      args=(table, columnfamily_cfid))
                    threads[table].start()

            for i in threads:
                threads[i].join()

    def add_host_status(self, host_id):
        host_status = HostStatus(host_id, self)
        self.host_statuses[host_id] = host_status
        return host_status

    def latest_restore_timestamp(self):
        restore_timestamps = []
        for host in self.host_statuses:
            restore_timestamps.append(self.host_statuses[host].latest_restore_timestamp())
        restore_timestamps = [rt for rt in restore_timestamps if rt is not None]
        if not restore_timestamps:
            return None
        return min(restore_timestamps)

    def status_output_by_host(self):
        output = {}
        for host in self.host_statuses:
            host_output = self.host_statuses[host].status_output()
            if host_output is not None:
                output[host] = host_output

        return output

    def status_output(self):
        output = []
        for host in self.host_statuses:
            host_output = self.host_statuses[host].status_output()
            if host_output is not None:
                output.append(self.host_statuses[host])
                output += host_output

        indented_output = ''

        for item in output:
            if isinstance(item, HostStatus):
                indented_output += 'Host {0}\n'.format(item.host_id)
            if isinstance(item, KeyspaceStatus):
                indented_output += '  Keyspace {0}\n'.format(item.name)
            if isinstance(item, ColumnfamilyStatus):
                indented_output += '    CF {0}\n'.format(item.name)
            if isinstance(item, SnapshotFileStatus):
                created = to_human_readable_time(item.created_timestamp)
                snapshot_created = to_human_readable_time(item.snapshot_owner.snapshot_timestamp)
                indented_output += '      {0} (created {1} - snapshot time {2})\n'.format(item.remote_path, created,
                                                                                          snapshot_created)
            if isinstance(item, IncrementalFileStatus):
                created = to_human_readable_time(item.created_timestamp)
                indented_output += '      {0} (created {1})\n'.format(item.remote_path, created)

        return indented_output


class HostStatus(object):
    def __init__(self, host_id, backup_status):
        self.host_id = host_id
        self.keyspace_statuses = {}
        self.backup_status = backup_status

    def add_keyspace_status(self, name):
        keyspace_status = KeyspaceStatus(name, self)
        self.keyspace_statuses[name] = keyspace_status
        return keyspace_status

    def latest_restore_timestamp(self):
        restore_timestamps = []
        for ks in self.keyspace_statuses:
            restore_timestamps.append(self.keyspace_statuses[ks].latest_restore_timestamp())
        restore_timestamps = [rt for rt in restore_timestamps if rt is not None]
        if not restore_timestamps:
            return None
        return min(restore_timestamps)

    def status_output(self):
        output = []
        for ks in self.keyspace_statuses:
            ks_output = self.keyspace_statuses[ks].status_output()
            if ks_output:
                output.append(self.keyspace_statuses[ks])
                output += ks_output
        return output


class KeyspaceStatus(object):
    name = None
    host_owner = None

    def __init__(self, name, host_owner):
        self.name = name
        self.host_owner = host_owner
        self.columnfamily_statuses = {}
        self.columnfamily_statuses_by_cfid = {}

    def add_columnfamily_status(self, columnfamily_name, columnfamily_cfid):
        logging.info('BackupStatus: Adding columnfamily status container for: {0}'.format(columnfamily_name))
        columnfamily_status = ColumnfamilyStatus(columnfamily_name, columnfamily_cfid, self)
        logging.info('BackupStatus: Columnfamily {0} status container complete.'.format(columnfamily_name))
        self.columnfamily_statuses[columnfamily_name] = columnfamily_status
        self.columnfamily_statuses_by_cfid[columnfamily_cfid] = columnfamily_status
        return columnfamily_status

    def latest_restore_timestamp(self):
        restore_timestamps = []
        for cf in self.columnfamily_statuses:
            restore_timestamps.append(self.columnfamily_statuses[cf].latest_restore_timestamp())
        restore_timestamps = [rt for rt in restore_timestamps if rt is not None]
        if not restore_timestamps:
            return None
        return min(restore_timestamps)

    def status_output(self):
        output = []
        for cf in self.columnfamily_statuses:
            cf_output = self.columnfamily_statuses[cf].status_output()
            if cf_output:
                output.append(self.columnfamily_statuses[cf])
                output += cf_output
        return output


class ColumnfamilyStatus(object):
    """
    Details about the state of the columnfamily including the latest snapshot, snapshot and incremental objects, and a
    reference to the parent KeyspaceStatus and parent BackupStatus.
    """

    def __init__(self, name, columnfamily_cfid, ks_owner):
        """
        Init.

        :param str name: columnfamily name.
        :param str cfid: columnfamily name with suffix hyphen columnfamily id hash.
        :param KeyspaceStatus ks_owner: KeyspaceStatus object parent of this ColumnFamilyStatus object.
        """
        self.name = name
        self.columnfamily_cfid = columnfamily_cfid
        self.ks_owner = ks_owner
        self.host_owner = ks_owner.host_owner
        self.snapshot_statuses = {}
        self.incremental_status = None

        self.backup_status = self.ks_owner.host_owner.backup_status
        self.manifest_manager = self.backup_status.manifest_manager

        self.manifest = self.manifest_manager.load_manifest(self.ks_owner.name, self.name)

        self.latest_snapshot = None

        if 'full' in self.manifest:
            for snapshot in self.manifest['full']:
                logging.info('BackupStatus: Adding snapshot status for columnfamily {0}.'.format(columnfamily_cfid))
                snapshot_status = self.add_snapshot_status(snapshot, self.manifest['full'][snapshot])
                logging.info('BackupStatus: Snapshot status for columnfamily {0} complete.'.format(columnfamily_cfid))

        if 'incremental' in self.manifest:
            logging.info('BackupStatus: Adding incremental status for columnfamily {0}.'.format(columnfamily_cfid))
            incremental_status = self.add_incremental_status(self.manifest['incremental'])
            logging.info('BackupStatus: Incremenatal status for columnfamily {0} complete.'.format(columnfamily_cfid))

    def add_snapshot_status(self, name, manifest_data):
        snapshot_status = SnapshotStatus(name, manifest_data, self)
        self.snapshot_statuses[name] = snapshot_status

        if snapshot_status.available_on_remote and snapshot_status.before_restore_time:
            if self.latest_snapshot is None:
                self.latest_snapshot = snapshot_status
            else:
                if snapshot_status.snapshot_timestamp > self.latest_snapshot.snapshot_timestamp:
                    self.latest_snapshot = snapshot_status

        return snapshot_status

    def add_incremental_status(self, manifest_data):
        if self.incremental_status is not None:
            raise RuntimeError('Incremental status is already set.')

        incremental_status = IncrementalStatus(manifest_data, self)
        self.incremental_status = incremental_status
        return incremental_status

    def latest_restore_timestamp(self):
        restore_timestamps = []
        if self.latest_snapshot:
            restore_timestamps.append(self.latest_snapshot.snapshot_timestamp)
        if self.incremental_status:
            restore_timestamps.append(self.incremental_status.latest_restore_timestamp())

        restore_timestamps = [rt for rt in restore_timestamps if rt is not None]
        if not restore_timestamps:
            return None
        return max(restore_timestamps)

    def status_output(self):
        output = []
        if self.latest_snapshot:
            latest_snapshot_output = self.latest_snapshot.output_status()
            if latest_snapshot_output:
                output += latest_snapshot_output
        if self.incremental_status:
            incremental_status_output = self.incremental_status.output_status()
            if incremental_status_output:
                output += incremental_status_output
        return output


class SnapshotStatus(object):
    """
    Details about the state of a snapshot for a columnfamily including whether this snapshot occurred before the parent
    object's requested restore time, if the snapshot is available on remote storage, and a dictionary of files included
    in this snapshot.
    """

    def __init__(self, name, manifest_data, cf_owner):
        """
        Init.

        :param str name: name of snapshot (integer timestamp of when snapshot occurred).
        :param dict manifest_data: manifest data as generated by this script.
        :param ColumnfamilyStatus cf_owner: ColumnfamilyStatus parent object which contains this SnapshotStatus object.
        """
        self.name = name
        # The name of the snapshot is the snapshot timestamp as the created timestamps of files can be before snapshot.
        self.snapshot_timestamp = int(self.name) / 1000
        self.manifest_data = manifest_data
        self.cf_owner = cf_owner
        self.snapshot_file_statuses = {}
        self.available_on_remote = False
        if self.cf_owner.backup_status.restore_time is None:
            self.before_restore_time = True
        else:
            self.before_restore_time = self.snapshot_timestamp <= self.cf_owner.backup_status.restore_time

        self.backup_repo = self.cf_owner.ks_owner.host_owner.backup_status.backup_repo

        remote_files = self.backup_repo.list_snapshot_files(self.cf_owner.ks_owner.host_owner.host_id,
                                                            self.cf_owner.ks_owner.name,
                                                            self.cf_owner.columnfamily_cfid,
                                                            self.name)

        for filename in manifest_data:
            if filename == 'manifest.json':
                continue

            created_timestamp = from_human_readable_time(manifest_data[filename]['created'])
            available_on_remote = remote_files is not None and filename in remote_files
            self.add_snapshot_file_status(filename, created_timestamp, available_on_remote)

        self.available_on_remote = self.snapshot_file_statuses != {} and all(
            [self.snapshot_file_statuses[aor].available_on_remote for aor in self.snapshot_file_statuses]
        )

    def add_snapshot_file_status(self, filename, created_timestamp, available_on_remote):
        snapshot_file_status = SnapshotFileStatus(filename, created_timestamp, available_on_remote, self)
        self.snapshot_file_statuses[filename] = snapshot_file_status
        return snapshot_file_status

    def output_status(self):
        output = []
        for snapshot_file in self.snapshot_file_statuses:
            snapshot_file_status = self.snapshot_file_statuses[snapshot_file]
            if snapshot_file_status.available_on_remote:
                output.append(snapshot_file_status)
        return output


class SnapshotFileStatus(object):
    """
    Object describing a single snapshot file status with a reference to the parent SnapshotStatus object.
    """

    def __init__(self, filename, created_timestamp, available_on_remote, snapshot_owner):
        """
        Init.

        :param str filename: filename.
        :param int created_timestamp:
        :param bool available_on_remote: if file is available on remote storage.
        :param SnapshotStatus snapshot_owner: SnapshotStatus parent object containing this SnapshotFileStatus object.
        """
        self.filename = filename
        self.available_on_remote = available_on_remote
        self.created_timestamp = created_timestamp
        self.snapshot_owner = snapshot_owner
        self.remote_path = '{host_id}/{ks}/{cfid}/snapshots/{snapshot}/{filename}'.format(
            host_id=self.snapshot_owner.cf_owner.ks_owner.host_owner.host_id,
            ks=self.snapshot_owner.cf_owner.ks_owner.name,
            cfid=self.snapshot_owner.cf_owner.columnfamily_cfid,
            snapshot=self.snapshot_owner.name,
            filename=filename
        )


class IncrementalStatus(object):
    """
    Object containing a dictionary of IncrementalFileStatus objects and a reference to the ColumnfamilyStatus parent.
    """

    def __init__(self, manifest_data, cf_owner):
        """
        Generate dict describing incremental file status objects.

        :param dict manifest_data: manifest data dict as generated by this script.
        :param ColumnfamilyStatus cf_owner: ColumnfamilyStatus parent object containing this IncrementalStatus object.
        """
        self.manifest_data = manifest_data
        self.cf_owner = cf_owner
        self.backup_repo = self.cf_owner.ks_owner.host_owner.backup_status.backup_repo
        self.incremental_file_statuses = {}

        s = time.time()
        remote_incrementals = self.backup_repo.list_backup_files(self.cf_owner.ks_owner.host_owner.host_id,
                                                                 self.cf_owner.ks_owner.name,
                                                                 self.cf_owner.columnfamily_cfid)

        logging.info('BackupStatus: Download Time {0}'.format(int(time.time() - s)))

        try:
            generation = cf_owner.latest_snapshot.manifest_data.keys()[0].split('-')[1]
        except AttributeError:
            generation = 0

        pre = len(remote_incrementals)
        remote_incrementals = filter(lambda n: (n[0] != '.' and n.split('-')[1] >= generation), remote_incrementals)
        post = len(remote_incrementals)
        logging.info('BackupStatus: Reduced list size from {0} to {1}: {2} less.'.format(pre, post, pre - post))

        # Separate list of remote incrementals into individual lists by file.
        suffixes = ['big-CompressionInfo.db', 'big-Data.db', 'big-Digest.adler32', 'big-Filter.db', 'big-Index.db',
                    'big-Statistics.db', 'big-Summary.db', 'big-TOC.txt', 'big-Digest.crc32']
        suffix_remote_incrementals = {}
        for suffix in suffixes:
            suffix_remote_incrementals[suffix] = set([ri for ri in remote_incrementals if ri.endswith(suffix)])

        s = time.time()
        for filename in manifest_data:
            created_timestamp = from_human_readable_time(manifest_data[filename]['created'])
            # Only incremental files created after the latest snapshot are needed.
            if self.cf_owner.latest_snapshot and created_timestamp <= self.cf_owner.latest_snapshot.snapshot_timestamp:
                continue

            logging.info('BackupStatus: Checking if {0} available on remote.'.format(filename))
            suffix = '-'.join(filename.split('-')[-2:])
            available_on_remote = remote_incrementals is not None and filename in suffix_remote_incrementals[suffix]
            logging.info('BackupStatus: Remote availability of {0}: {1}'.format(filename, available_on_remote))

            self.add_incremental_file_status(filename, created_timestamp, available_on_remote)
        logging.info('BackupStatus: Check Time {0}'.format(int(time.time() - s)))

    def add_incremental_file_status(self, filename, created_timestamp, available_on_remote):
        incremental_file_status = IncrementalFileStatus(filename, created_timestamp, available_on_remote, self)
        self.incremental_file_statuses[filename] = incremental_file_status
        return incremental_file_status

    def latest_restore_timestamp(self):
        restore_timestamps = []
        for incremental in self.incremental_file_statuses:
            incremental_status = self.incremental_file_statuses[incremental]
            if incremental_status.available_on_remote:
                restore_timestamps.append(incremental_status.created_timestamp)

        restore_timestamps = [rt for rt in restore_timestamps if rt is not None]
        if not restore_timestamps:
            return None
        return max(restore_timestamps)

    def output_status(self):
        output = []
        for incremental_file in self.incremental_file_statuses:
            incremental_file_status = self.incremental_file_statuses[incremental_file]
            if incremental_file_status.available_on_remote and incremental_file_status.before_restore_time:
                output.append(incremental_file_status)
        return output


class IncrementalFileStatus(object):
    def __init__(self, filename, created_timestamp, available_on_remote, incremental_owner):
        self.filename = filename
        self.available_on_remote = available_on_remote
        self.created_timestamp = created_timestamp
        self.incremental_owner = incremental_owner

        if self.incremental_owner.cf_owner.backup_status.restore_time is None:
            self.before_restore_time = True
        else:
            self.before_restore_time = self.created_timestamp <= self.incremental_owner.cf_owner.backup_status.restore_time

        self.remote_path = '{host_id}/{ks}/{cfid}/backups/{filename}'.format(
            host_id=self.incremental_owner.cf_owner.ks_owner.host_owner.host_id,
            ks=self.incremental_owner.cf_owner.ks_owner.name,
            cfid=self.incremental_owner.cf_owner.columnfamily_cfid,
            filename=filename
        )


class BackupManager(object):
    """
    The BackupManager facilitates Cassandra backup and upload to remote storage repositories.
    """

    cassandra = None
    backup_repo = None
    manifest_manager = None

    def __init__(self, cassandra, backup_repo, manifest_manager):
        """
        Initiate the BackupManager class.

        :param Cassandra cassandra: Cassandra information resource.
        :param BaseBackupRepo backup_repo: remote storage repository.
        :param ManifestManager manifest_manager: manifest manager.
        """
        self.cassandra = cassandra
        self.backup_repo = backup_repo
        self.manifest_manager = manifest_manager

    def full_backup(self, columnfamily=None, thread_limit=4):
        """
        Run a full backup (snapshot) on this cassandra node and upload it to remote storage.

        :param str columnfamily: optionally perform full backup on only this keyspace and columnfamily.
        """
        self.manifest_manager.update_host_list()

        logging.info('Flushing node.')
        self.cassandra.nodetool_flush()

        snapshot_start = time.time()
        snapshot_name = str(int(round(snapshot_start * 1000)))
        logging.info('Starting snapshot with name: {0}'.format(snapshot_name))
        self.cassandra.nodetool_snapshot(snapshot_name, columnfamily)

        if self.backup_repo:
            try:
                self.manifest_manager.download_manifests(self.cassandra.host_id)
                self.manifest_manager.update_snapshot_manifests(snapshot_name, columnfamily)
                self.manifest_manager.upload_manifests(self.cassandra.host_id)

                self.backup_repo.upload_snapshot(self.cassandra.host_id, self.cassandra.data_file_directories,
                                                 snapshot_name, thread_limit)
            except Exception as exception:
                logging.warning('Exception when uploading during full_backup: {0}'.format(exception))
                raise exception
            finally:
                logging.info('Clearing snapshot {0} data'.format(snapshot_name))
                self.cassandra.nodetool_clearsnapshot(snapshot_name)

        logging.info('Finished snapshot after {0} seconds.'.format(int(time.time() - snapshot_start)))

    def __find_incremental_files(self, data_file_directory, columnfamily=None):
        """
        Find the incremental files that need to be uploaded, and create a dictionary of the files and the surrounding
        directories.

        :param str columnfamily: optionally perform incremental backup on only this keyspace and columnfamily.
        """
        logging.info('Finding incremental files in {0}'.format(data_file_directory))

        incremental_files = {}
        # incremental files should be a list of things like <ks>/<cf>/backups/<sstable>
        for root, dirs, files in os.walk(data_file_directory):
            # Find each <ks>/<cf>/backups/<sstable>
            if root.endswith('/backups'):
                root = root[len(data_file_directory):]
                for file in files:
                    if root not in incremental_files.keys():
                        incremental_files[root] = []
                    filename = os.path.join(root, file)
                    incremental_files[root].append(filename)
            if '/backups' in root and '/.' in root and root.endswith('_idx'):
                root = root[len(data_file_directory):] + '/'
                for file in files:
                    if root not in incremental_files.keys():
                        incremental_files[root] = []
                    filename = os.path.join(root, file)
                    incremental_files[root].append(filename)

        path_filter = None
        if columnfamily is not None:
            keyspace, columnfamily = columnfamily.split('.')
            path_filter = '{0}/{1}'.format(keyspace, columnfamily)

        if path_filter is not None:
            incremental_files = {
                i: incremental_files[i] for i in incremental_files if i.startswith(path_filter)
            }

        return incremental_files

    @filelocked('/tmp/.incremental_cassandra_backup')
    def incremental_backup(self, columnfamily=None, thread_limit=4):
        """
        Sync incremental backups that are stored on this cassandra node and upload them to remote storage. This will
        remove incremental files after uploading.

        :param str columnfamily: optionally perform incremental backup on only this keyspace and columnfamily.
        """
        self.manifest_manager.update_host_list()

        incremental_start = time.time()
        logging.info('Starting incremental backup.')
        if not self.cassandra.backupenabled:
            logging.critical('You must enable backups to use this feature.')
            raise RuntimeError('Backups are not enabled.')

        for ks in self.cassandra.keyspace_schema_data:
            for cf in self.cassandra.keyspace_schema_data[ks]['tables']:
                ks_cf = '{ks}.{cf}'.format(ks=ks, cf=cf)
                try:
                    self.manifest_manager.download_manifests(self.cassandra.host_id, ks_cf)
                except Exception as exception:
                    logging.info('Incremental manifest download for {0} error: {1}'.format(ks_cf, exception))

        all_incremental_files = []
        for data_file_directory in self.cassandra.data_file_directories:
            incremental_files = self.__find_incremental_files(data_file_directory, columnfamily)
            all_incremental_files.append((data_file_directory, incremental_files))
            updated_manifest_files = self.manifest_manager.incremental_manifest(data_file_directory, incremental_files)

        for ks in self.cassandra.keyspace_schema_data:
            for cf in self.cassandra.keyspace_schema_data[ks]['tables']:
                ks_cf = '{ks}.{cf}'.format(ks=ks, cf=cf)
                try:
                    self.manifest_manager.upload_manifests(self.cassandra.host_id, ks_cf)
                except Exception as exception:
                    logging.info('Incremental manifest upload for {0} error: {1}'.format(ks_cf, exception))

        for data_file_directory in self.cassandra.data_file_directories:
            files_to_upload = []
            for path in incremental_files:
                files_to_upload.append(path + '/')

            logging.info('Preparing to upload {0} files using {1} threads.'.format(len(files_to_upload), thread_limit))
            for ti in range(0, len(files_to_upload), thread_limit):
                files_to_upload_subset = files_to_upload[ti:ti + thread_limit]

                logging.info('Starting {0} threads.'.format(len(files_to_upload_subset)))
                upload_threads = []
                for files_to_upload_subset_item in files_to_upload_subset:
                    upload_thread = threading.Thread(target=self.backup_repo.upload_incremental_backups, args=(
                        self.cassandra.host_id, data_file_directory, files_to_upload_subset_item))
                    upload_threads.append(upload_thread)
                    upload_thread.start()

                for upload_thread in upload_threads:
                    upload_thread.join()

        logging.info('Clearing incremental files.')
        for data_file_directory, incremental_files in all_incremental_files:
            self.cassandra.clear_incrementals(data_file_directory, incremental_files.items())

        logging.info('Finished incremental backup after {0} seconds.'.format(int(time.time() - incremental_start)))

    def status(self, columnfamily, restore_time, quiet):
        """
        Output the latest available backup time and associated files from the backup repository for this host.

        :param str columnfamily: optionally only get status for this keyspace and columnfamily.
        :param int restore_time: timestamp for latest time which can be used to determine restore status and operations.
        :param bool quiet: optionally hide file status output and only print restore time.
        """
        backup_status = BackupStatus(self.manifest_manager, self.backup_repo, restore_time, columnfamily)
        logging.info(backup_status.status_output())
        if not quiet:
            print backup_status.status_output()

        if backup_status.latest_restore_timestamp():
            output = 'Restore time: {0}'.format(to_human_readable_time(backup_status.latest_restore_timestamp()))
        else:
            output = 'Restore time: N/A'

        logging.info(output)
        print output

        return backup_status

    @filelocked(lambda: RESTORE_DIR_FILELOCK)
    def restore(self, columnfamily, nodes, restore_time=None, restore_dir=None, host_ids=None, username=None,
                password=None):
        """
        Restore backup of columnfamily to provided nodes. This will use sstableloader to stream data after downloading
        from the selected backup repository.

        Optionally provide a restore_time to restore up to that point.
        Optionally provide a restore_dir to use a restore directory other than tmp to download and organize files.
        Optionally provide host_ids to only restore data from selected hosts.

        :param str columnfamily: dot separated keyspace and columnfamily.
        :param str nodes: comma separated list of nodes for sstableloader to connect to.
        :param str restore_time: time to restore to.
        :param str restore_dir: directory to use for restore download and file organization.
        :param list[str] host_ids: list of host_ids to filter for restore.
        :param str username: optional username to provide sstableloader.
        :param str password: optional password to provide sstableloader.
        """
        restore_start = time.time()

        restore_dir = restore_dir.rstrip('/')

        logging.info('Acquiring status for {0}.'.format(columnfamily))
        backup_status = BackupStatus(self.manifest_manager, self.backup_repo, restore_time, columnfamily, host_ids)

        status_output_by_host = backup_status.status_output_by_host()

        for host in status_output_by_host:
            snapshot_files_to_download = []
            backup_files_to_download = []

            for item in status_output_by_host[host]:
                if isinstance(item, SnapshotFileStatus) or isinstance(item, IncrementalFileStatus):
                    remote_path = item.remote_path
                    if isinstance(item, SnapshotFileStatus):
                        snapshot_path = remote_path[0:remote_path.index('/snapshots/')]
                        snapshot_name = item.snapshot_owner.name
                        remote_path = '{0}/snapshots/{1}/*'.format(snapshot_path, snapshot_name)
                        snapshot_files_to_download.append(remote_path)
                    else:
                        backup_files_to_download.append(remote_path)

            snapshot_files_to_download = list(set(snapshot_files_to_download))
            backup_files_to_download = glob_optimize_backup_paths(backup_files_to_download)
            files_to_download = snapshot_files_to_download + backup_files_to_download

            logging.info('Clearing restore directory: {0}'.format(restore_dir))
            run_command(['rm', '-rf', restore_dir])

            logging.info('Starting downloads for restore.')

            for file_to_download in files_to_download:
                remote_split = file_to_download.split('/')
                local_path = '{0}/download/{1}/'.format(restore_dir, '/'.join(remote_split[-6:-1]))
                logging.info('Downloading: {0}'.format(file_to_download))
                self.backup_repo.download_files(file_to_download, local_path)

            logging.info('Download complete.')

            # Move files
            host_status = backup_status.host_statuses[host]

            for ks in host_status.keyspace_statuses:
                ks_status = host_status.keyspace_statuses[ks]
                for cf in ks_status.columnfamily_statuses:
                    cf_status = ks_status.columnfamily_statuses[cf]
                    cfid = cf_status.columnfamily_cfid
                    snapshot = cf_status.latest_snapshot
                    if snapshot is not None:
                        snapshot_id = snapshot.name
                        downloaded_path = '{0}/download/{1}/{2}/{3}/snapshots/{4}'.format(
                            restore_dir, host, ks, cfid, snapshot_id)
                        restore_path = '{0}/{1}/{2}/'.format(restore_dir, ks, cf)

                        downloaded_files = os.listdir(downloaded_path)
                        for downloaded_file in downloaded_files:
                            downloaded_file_full_path = os.path.join(downloaded_path, downloaded_file)
                            if not os.path.exists(os.path.dirname(restore_path)):
                                os.makedirs(os.path.dirname(restore_path))
                            os.rename(downloaded_file_full_path, '{0}{1}'.format(restore_path, downloaded_file))

                    incremental = cf_status.incremental_status
                    if incremental is not None:
                        for incremental_file_status in incremental.incremental_file_statuses:
                            incremental_file_status = incremental.incremental_file_statuses[incremental_file_status]
                            if not incremental_file_status.before_restore_time:
                                continue

                            filename = incremental_file_status.filename
                            downloaded_path = '{0}/download/{1}/{2}/{3}/backups/{4}'.format(
                                restore_dir, host, ks, cfid, filename)
                            restore_path = '{0}/{1}/{2}/{3}'.format(restore_dir, ks, cf, filename)
                            if not os.path.exists(os.path.dirname(restore_path)):
                                os.makedirs(os.path.dirname(restore_path))

                            if os.path.exists(downloaded_path):
                                os.rename(downloaded_path, restore_path)
                            else:
                                logging.warning('Incremental file in manifest does not exist: {0}.'.format(
                                    downloaded_path))

            if username is None and password is None:
                try:
                    config = ConfigParser.ConfigParser()
                    config.read(os.path.expanduser('~')+'/.cassandra/cqlshrc')
                    username = config.get('authentication', 'username')
                    password = config.get('authentication', 'password')
                except ConfigParser.NoSectionError:
                    pass
                except ConfigParser.NoOptionError:
                    pass

            for ks in host_status.keyspace_statuses:
                ks_status = host_status.keyspace_statuses[ks]
                for cf in ks_status.columnfamily_statuses:
                    cmd = ['sstableloader', '-d', nodes, '{0}/{1}/{2}'.format(restore_dir, ks, cf)]
                    if username and password:
                        cmd += ['-u', username, '-pw', password]
                    return_code, out, err = run_command(cmd)
                    logging.info('Output for sstableloader restore to {0}:\n{1}'.format(nodes, out))

        logging.info('Finished restore after {0} seconds.'.format(int(time.time() - restore_start)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    main_subparsers = parser.add_subparsers(help='action help')

    for action in ['full', 'incremental', 'status', 'restore']:
        action_parser = main_subparsers.add_parser(action, help='{0} help'.format(action))
        action_parser.set_defaults(action=action)
        action_subparser = action_parser.add_subparsers(help='{0} help'.format(action))
        for repo in (AWSBackupRepo,):
            repo_parser = repo.build_parser(action_subparser)
            # Cassandra options
            repo_parser.add_argument('--cassandra-config', dest='cassandra_config',
                                     default='/etc/cassandra/conf/cassandra.yaml',
                                     help='Place to find the cassandra configuration file')
            columnfamily_arg = repo_parser.add_argument(
                '--columnfamily', help='Only execute backup on specified columnfamily. Must include keyspace in the '
                                       'format: <keyspace>.<columnfamily>')
            # cqlsh options
            repo_parser.add_argument('--cqlsh-host', dest='cqlsh_host', required=False, default=socket.getfqdn(),
                                     help='Sets the cqlsh host that will be used to run cqlsh commands')
            repo_parser.add_argument('--cqlsh-ssl', dest='cqlsh_ssl', required=False, default=False, action='store_true',
                                     help='Uses SSL when connecting to CQLSH')
            repo_parser.add_argument('--cqlsh-user', dest='cqlsh_user', required=False,
                                     help='Optionally provide username to use when connecting to CQLSH')
            repo_parser.add_argument('--cqlsh-pass', dest='cqlsh_pass', required=False,
                                     help='Optionally provide password to use when connecting to CQLSH')
            # Manifest options
            repo_parser.add_argument('--retention-days', dest='retention_days', required=False,
                                     help='Optionally provide how many days to retain manifest data')
            # Debugging
            repo_parser.add_argument('--dry-run', dest='dry_run', action='store_true', default=False,
                                     help='Instead of running commands, print simulated commands that would have run.')
            repo_parser.add_argument('--debug', action='store_true', default=False, dest='debug',
                                     help='Enable verbose DEBUG level logging.')
            repo_parser.add_argument('--log-to-file', help='Redirect all logging to file. Output is not redirected.')
            repo_parser.add_argument('--meta-path', help='Path for which to store meta JSON data.',
                                     default='/tmp/onzra_casandra_backup_service')
            repo_parser.add_argument('--thread-limit', type=int, help='Maximum number of concurrent threads.',
                                     default=4)

            if action == 'status':
                columnfamily_arg.required = True
                repo_parser.add_argument('--restore-time', help='UTC timestamp in seconds to get status up to.')
                repo_parser.add_argument('--quiet', action='store_true', help='Hide file output and only print time.')

            if action == 'restore':
                columnfamily_arg.required = True
                repo_parser.add_argument('--destination-nodes', help='Connect to a list of (comma separated) hosts for '
                                                                     'initial cluster information', required=True)
                repo_parser.add_argument('--restore-time', help='UTC timestamp in seconds to restore nodes to.')
                repo_parser.add_argument('--restore-dir', default='/tmp/restore',
                                         help='Temporary directory to use for downloading and restoring files. This '
                                              'directory will be destroyed and recreated during the restore process.')
                repo_parser.add_argument('--limit-host-ids', help='Comma separated list of hosts to filter a restore.')
                repo_parser.add_argument('--username', help='Username with restore privileges for sstableloader.')
                repo_parser.add_argument('--password', help='Password with restore privileges for sstableloader.')

    args = parser.parse_args()

    if args.dry_run:
        DRY_RUN = True

    if args.debug:
        logger.setLevel(logging.DEBUG)

    if args.log_to_file:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', filename=args.log_to_file)
    else:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')

    if args.meta_path:
        meta_path = args.meta_path

    if args.action in ('full', 'incremental'):
        cass = Cassandra(args)
        meta_path = cass.meta_path
    elif args.action in ('status', 'restore'):
        cass = None
        meta_path = tempfile.mkdtemp()

    if args.repo is AWSBackupRepo:
        repo = AWSBackupRepo(meta_path, args.s3_bucket, args.s3_metadata_bucket, args.s3_storage_class, args.s3_sse)

    manifest_manager = ManifestManager(cass, meta_path, repo, args.retention_days)
    backup_manager = BackupManager(cass, repo, manifest_manager)

    try:
        if args.action == 'full':
            backup_manager.full_backup(args.columnfamily)
            full_status_file = '/tmp/onzra_cassandra_backup_service-full.status'
            with open(full_status_file, 'a'):
                os.utime(full_status_file, None)
            os.chmod(full_status_file, stat.S_IRWXO | stat.S_IRWXG | stat.S_IRWXU)
        elif args.action == 'incremental':
            try:
                backup_manager.incremental_backup(args.columnfamily, args.thread_limit)
            except FileLockedError as file_locked_error:
                logging.warning('Incremental backup in progress using {0} lock file.'.format(file_locked_error))
                exit(10)
        elif args.action == 'status':
            backup_manager.status(args.columnfamily, args.restore_time, args.quiet)
        elif args.action == 'restore':
            if not os.path.exists(args.restore_dir):
                os.makedirs(args.restore_dir)
            RESTORE_DIR_FILELOCK = '{0}.filelock'.format(args.restore_dir)
            host_ids = args.limit_host_ids.split(',') if args.limit_host_ids else None
            try:
                backup_manager.restore(args.columnfamily, args.destination_nodes, args.restore_time, args.restore_dir,
                                       host_ids, args.username, args.password)
            except FileLockedError as file_locked_error:
                logging.warning('Restore in progress using {0} lock file.'.format(file_locked_error))
                exit(10)

    except Exception as exception:
        logging.exception('Exception during action: {0}'.format(args.action))
        raise

    if args.action in ('status', 'restore'):
        run_command(['rm', '-rf', meta_path])

    exit(0)
