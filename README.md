# Overview

This code should be checked out into a location on each Cassandra machine you
wish to participate in backups. For the purposes of this document we will assume
this was placed in `/usr/local/cassandra_backup_service/`

The status action can be executed on a machine without Cassandra.

## Configuration Overview:

When running the cassandra_backup_service.py tool, you will specify an action, and a
supported repository to store the  backups.

Supported Actions:

- full
- incremental
- status
- restore

Supported Repositories:

- aws (beta)
- gcp (development)

Note: Below examples will currently only explain the AWS implementation until
the GCP version is complete

```
$ /usr/local/cassandra_backup_service/cassandra_backup_service.py --help
usage: cassandra_backup_service.py [-h] {full,incremental,status,restore} ...

positional arguments:
  {full,incremental,status,restore}
                        action help
    full                full help
    incremental         incremental help
    status              status help
    restore             restore help

optional arguments:
  -h, --help            show this help message and exit
```

Note: All of the actions assume cqlsh can be ran on this node by the user with

### AWS Configuration:

The AWS implementation uses AWS CLI tools which is installed via pip:

    pip install awscli

The credentials for awscli are stored here:

    cat ~/.aws/credentials
    [default]
    aws_access_key_id = ABCDEFG1AB1234ABC1AB
    aws_secret_access_key = ABaABCa+/a1aA1aAaABaAab+AaAB2abcA1AaABCa

### Configuration Full Backups

In most environments we usually recommend that full backups are taken weekly.

```
0 8 * * 0 root /usr/local/cassandra_backup_service/cassandra_backup_service.py --cqlsh-host `hostname` full aws --aws-s3-bucket s3://bucket/
```


#### Help


```
$ /usr/local/cassandra_backup_service/cassandra_backup_service.py full aws --help
usage: cassandra_backup_service.py full aws [-h] --aws-s3-bucket S3_BUCKET
                                          [--aws-s3-sse S3_SSE]
                                          [--cassandra-config CASSANDRA_CONFIG]
                                          [--columnfamily COLUMNFAMILY]
                                          [--cqlsh-host CQLSH_HOST]
                                          [--dry-run] [--debug]

optional arguments:
  -h, --help            show this help message and exit
  --aws-s3-bucket S3_BUCKET
                        The AWS S3 Directory to upload this backup to.
  --aws-s3-sse S3_SSE   Use SSE for the connection to S3
  --cassandra-config CASSANDRA_CONFIG
                        Place to find the cassandra configuration file
  --columnfamily COLUMNFAMILY
                        Only execute backup on specified columnfamily. Must
                        include keyspace in the format:
                        <keyspace>.<columnfamily>
  --cqlsh-host CQLSH_HOST
                        Sets the cqlsh host that will be used to run cqlsh
                        commands
  --dry-run             Instead of running commands, it prints simulated
                        commands that would have run.
  --debug               Enable verbose DEBUG level logging.
```

### Configuration Incremental Backups

#### Example Cron

```
* * * * * root cd /usr/local/cassandra_backup_service/cassandra_backup_service.py --cqlsh-host `hostname` incremental aws --aws-s3-bucket s3://bucket/
```

#### Help

```
$ /usr/local/cassandra_backup_service/cassandra_backup_service.py incremental aws --help
usage: cassandra_backup_service.py incremental aws [-h] --aws-s3-bucket
                                                 S3_BUCKET
                                                 [--aws-s3-sse S3_SSE]
                                                 [--cassandra-config CASSANDRA_CONFIG]
                                                 [--columnfamily COLUMNFAMILY]
                                                 [--cqlsh-host CQLSH_HOST]
                                                 [--dry-run] [--debug]

optional arguments:
  -h, --help            show this help message and exit
  --aws-s3-bucket S3_BUCKET
                        The AWS S3 Directory to upload this backup to.
  --aws-s3-sse S3_SSE   Use SSE for the connection to S3
  --cassandra-config CASSANDRA_CONFIG
                        Place to find the cassandra configuration file
  --columnfamily COLUMNFAMILY
                        Only execute backup on specified columnfamily. Must
                        include keyspace in the format:
                        <keyspace>.<columnfamily>
  --cqlsh-host CQLSH_HOST
                        Sets the cqlsh host that will be used to run cqlsh
                        commands
  --dry-run             Instead of running commands, it prints simulated
                        commands that would have run.
  --debug               Enable verbose DEBUG level logging.
```

## Manual Actions

Manual actions can be taken to detect the current state of the uploaded backups
or restore a specific keyspace / column family combination.

### Checking status

#### Help
```
$ /usr/local/cassandra_backup_service/cassandra_backup_service.py status aws --help
usage: cassandra_backup_service.py status aws [-h] --aws-s3-bucket S3_BUCKET
                                            [--aws-s3-sse S3_SSE]
                                            [--cassandra-config CASSANDRA_CONFIG]
                                            [--columnfamily COLUMNFAMILY]
                                            [--cqlsh-host CQLSH_HOST]
                                            [--dry-run] [--debug]

optional arguments:
  -h, --help            show this help message and exit
  --aws-s3-bucket S3_BUCKET
                        The AWS S3 Directory to upload this backup to.
  --aws-s3-sse S3_SSE   Use SSE for the connection to S3
  --cassandra-config CASSANDRA_CONFIG
                        Place to find the cassandra configuration file
  --columnfamily COLUMNFAMILY
                        Only execute backup on specified columnfamily. Must
                        include keyspace in the format:
                        <keyspace>.<columnfamily>
  --cqlsh-host CQLSH_HOST
                        Sets the cqlsh host that will be used to run cqlsh
                        commands
  --dry-run             Instead of running commands, it prints simulated
                        commands that would have run.
  --debug               Enable verbose DEBUG level logging.
```

#### Example Output

```
python cassandra_backup_service.py status aws --aws-s3-bucket s3://tropical-box --columnfamily backup_service_test.incremental_test
Backup Time: Thu Jan 24 00:43:55 2019
Files:
- 94e99e4c-b2c0-4ed3-b191-494f43efe964
  - backup_service_test/incremental_test/lb-1-big-Data.db (created 2019-01-24 00:43:54)
```

## Testing with Vagrant

Executing `vagrant up` using the provided [Vagrantfile](Vagrantfile) will set up a Centos 7 Virtualbox and execute a
basic dry run of the script. This can be useful for quickly creating an initial simple environment for testing or for
confirming changes to the script during development.

The [Vagrantfile](Vagrantfile) will install Cassandra, execute the script, then test the result of the script using
[dry_run_full_test.py](dry_run_full_test.py).

The test is limited to Vagrant's provisioning process so running a new test will require a `vagrant destroy` of the
machine for a new test run. Alternatively, the commands to test from the [Vagrantfile](Vagrantfile) can be run manually
from the Vagrant box by shelling into the machine with `vagrant ssh` after it is up.
