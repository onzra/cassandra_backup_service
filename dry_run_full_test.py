import unittest
import json
from os import environ


class CassandraBackupServiceTest(unittest.TestCase):

    def test_full_dry_run_creates_system_auth_roles_manifest(self):
        """
        Test that execution of a full dry run has created a system_auth/roles manifest.json file.
        """
        meta_path = environ['META_PATH']
        with open("{0}/system_auth/roles/meta/manifest.json".format(meta_path), "r") as f:
            result = f.read()

        result_json = json.loads(result)
        self.assertEqual('roles', result_json['column_family'])
        self.assertEqual('system_auth', result_json['keyspace'])
        self.assertTrue('full' in result_json)


if __name__ == '__main__':
    unittest.main()