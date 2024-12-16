import unittest
from typing import Tuple
from src.main import DBTBlueGreen

class DbtBuildTest(unittest.TestCase):

    def setUp(self):
        # This is where you would instantiate the class that contains _make_select_exclude_statement
        # For example, if it's in a class named DbtBuild, you would do:
        self.bg = DBTBlueGreen(blue_database='TEST',
                               unit_test=True)


    def test_all_flags_true(self):
        select, exclude = self.bg._make_select_exclude_statement(
            do_snapshot=True, do_seed=True, do_run=True, do_test=True,
            snapshot_select='snapshot_model', snapshot_exclude='exclude_snapshot',
            seed_select='seed_model', seed_exclude='exclude_seed',
            run_select='run_model', run_exclude='exclude_run',
            test_select='test_model', test_exclude='exclude_test',
            manifest=False
        )
        self.assertEqual(select, 'resource_type:snapshot,snapshot_model resource_type:seed,seed_model resource_type:model,run_model resource_type:test,test_model')
        self.assertEqual(exclude, 'resource_type:snapshot,exclude_snapshot resource_type:seed,exclude_seed resource_type:model,exclude_run resource_type:test,exclude_test')

    def test_all_flags_false(self):
        select, exclude = self.bg._make_select_exclude_statement(
            do_snapshot=False, do_seed=False, do_run=False, do_test=False,
            snapshot_select='snapshot_model', snapshot_exclude='exclude_snapshot',
            seed_select='seed_model', seed_exclude='exclude_seed',
            run_select='run_model', run_exclude='exclude_run',
            test_select='test_model', test_exclude='exclude_test',
            manifest=False
        )
        self.assertEqual(select, '')
        self.assertEqual(exclude, 'resource_type:snapshot resource_type:seed resource_type:model resource_type:test')

    def test_some_flags_true(self):
        select, exclude = self.bg._make_select_exclude_statement(
            do_snapshot=True, do_seed=False, do_run=True, do_test=False,
            snapshot_select='snapshot_model', snapshot_exclude='exclude_snapshot',
            seed_select='seed_model', seed_exclude='exclude_seed',
            run_select='run_model', run_exclude='exclude_run',
            test_select='test_model', test_exclude='exclude_test',
            manifest=False
        )
        self.assertEqual(select, 'resource_type:snapshot,snapshot_model resource_type:model,run_model')
        self.assertEqual(exclude, 'resource_type:snapshot,exclude_snapshot resource_type:seed resource_type:model,exclude_run resource_type:test')

    def test_empty_select_exclude(self):
        select, exclude = self.bg._make_select_exclude_statement(
            do_snapshot=True, do_seed=True, do_run=True, do_test=True,
            snapshot_select='', snapshot_exclude='',
            seed_select='', seed_exclude='',
            run_select='', run_exclude='',
            test_select='', test_exclude='',
            manifest=False
        )
        self.assertEqual('resource_type:snapshot resource_type:seed resource_type:model resource_type:test', select)
        self.assertEqual('', exclude)

    def test_no_snapshot(self):
        select, exclude = self.bg._make_select_exclude_statement(
            do_snapshot=False, do_seed=True, do_run=True, do_test=True,
            snapshot_select='snapshot_model', snapshot_exclude='exclude_snapshot',
            seed_select='seed_model', seed_exclude='exclude_seed',
            run_select='tag:run_model tag:another_model', run_exclude='exclude_run',
            test_select='test_model', test_exclude='exclude_test',
            manifest=False
        )
        self.assertEqual(select, 'resource_type:seed,seed_model resource_type:model,tag:run_model resource_type:model,tag:another_model resource_type:test,test_model')
        self.assertEqual(exclude, 'resource_type:snapshot resource_type:seed,exclude_seed resource_type:model,exclude_run resource_type:test,exclude_test')

    def test_daily_run_config(self):
        select, exclude = self.bg._make_select_exclude_statement(
            do_snapshot=False, do_seed=True, do_run=True, do_test=True,
            snapshot_select='', snapshot_exclude='',
            seed_select='', seed_exclude='',
            run_select='tag:vs_daily_airbyte+', run_exclude='',
            test_select='', test_exclude='tag:unit-test tag:expectations',
            manifest=False
        )
        self.assertEqual(select, 'resource_type:seed resource_type:model,tag:vs_daily_airbyte+ resource_type:test')
        self.assertEqual(exclude, 'resource_type:snapshot resource_type:test,tag:unit-test resource_type:test,tag:expectations')


    # def test_multi_select_criteria(self):
    #     # This test makes sure that the split and resource type append works in all caes.
    #     select, exclude = self.bg._make_select_exclude_statement(
    #         do_snapshot=True, do_seed=True, do_run=True, do_test=True,
    #         snapshot_select='a z,x', snapshot_exclude='a b z,x',
    #         seed_select='a b z,x', seed_exclude='a b z,x',
    #         run_select='a b z,x', run_exclude='a b z,x',
    #         test_select='a b z,x', test_exclude='a b z,x',
    #         manifest=False
    #     )
    #     self.assertEqual(select, 'resource_type:seed,a resource_type:seed,z,x resource_type:model,tag:vs_daily_airbyte+ resource_type:test')
    #     self.assertEqual(exclude, 'resource_type:snapshot resource_type:test,tag:unit-test resource_type:test,tag:expectations')


    def test_manifest(self):
        select, exclude = self.bg._make_select_exclude_statement(
            do_snapshot=False, do_seed=True, do_run=True, do_test=True,
            snapshot_select='', snapshot_exclude='',
            seed_select='', seed_exclude='',
            run_select='', run_exclude='',
            test_select='', test_exclude='exclude_test',
            manifest=True
        )
        self.assertEqual('resource_type:seed resource_type:model,state:modified+ resource_type:test,state:modified+', select)
        self.assertEqual('resource_type:snapshot resource_type:test,exclude_test', exclude)

if __name__ == '__main__':
    unittest.main()
