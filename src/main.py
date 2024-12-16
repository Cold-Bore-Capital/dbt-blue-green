import os
import time
import re
import logging
from typing import List, Tuple, Optional

import snowflake.connector
import subprocess
from src.clone_database import CloneDB
from src.utilities import Utilities
from src.core import Core


class DBTBlueGreen(Core):

    def __init__(self,
                 blue_database: str = None,
                 green_database: str = None,
                 unit_test: bool = False,
                 thread_count: int = 20,
                 account: Optional[str] = None,
                 warehouse: Optional[str] = None,
                 database: Optional[str] = None,
                 role: Optional[str] = None,
                 schema: Optional[str] = None,
                 user: Optional[str] = None,
                 password: Optional[str] = None,
                 query_tag: Optional[str] = None):

        super().__init__(blue_database,
                         green_database,
                         thread_count,
                         account,
                         warehouse,
                         database,
                         role,
                         schema,
                         user,
                         password,
                         query_tag,
                         unit_test)

        self.logger = logging.getLogger(__name__)

        if not unit_test:
            self.con = self.snowflake_connection()
            self._thread_count = int(os.environ.get('DBT_THREAD_COUNT', '6'))
            self._stomp_on_green_timeout = int(os.environ.get('STOMP_ON_GREEN_TIMEOUT', 10))

        launch_root = Utilities.get_path_to_launch_root()[1:].split('/')[:-2]
        dbt_root = launch_root + ['transform']
        self._dbt_root = '/' + '/'.join(dbt_root)
        self.logger.info(f'Running DBT in {dbt_root}')

    def main(self,
             snapshot_select: str,
             snapshot_exclude: str,
             seed_select: str,
             seed_exclude: str,
             run_select: str,
             run_exclude: str,
             test_select: str,
             test_exclude: str,
             query_tag: Optional[str] = None,
             do_seed: bool = False,
             do_snapshot: bool = False,
             do_run: bool = False,
             do_test: bool = False,
             full_refresh: bool = False,
             no_swap: bool = False,
             drop_on_existing_db: bool = False,
             fail_fast: bool = False,
             dbt_target: str = None,
             stomp_on_green: bool = False
             ):
        """
        Main function to execute the blue green deployment process

        Args:
            do_snapshot: Boolean to determine if the snapshot command should be run
            do_seed: Boolean to determine if the seed command should be run
            do_run: Boolean to determine if the run command should be run
            do_test: Boolean to determine if the test command should be run
            snapshot_select: The models or tags to include in the snapshot command `--select`
            snapshot_exclude: The models or tags to exclude in the snapshot command `--exclude`
            seed_select: The models or tags to include in the seed command `--select`
            seed_exclude: The models or tags to exclude in the seed command `--exclude`
            seed_select: The models or tags to include in the seed command `--select`
            seed_exclude: The models or tags to exclude in the seed command `--exclude`
            run_select: The models or tags to include in the run command `--select`
            run_exclude: The models or tags to exclude in the run command `--exclude`
            test_select: The models or tags to include in the test command `--select`
            test_exclude: The models or tags to exclude in the test command `--exclude`
            query_tag: A name to identify the query when running in prod. `--query_tag`
            full_refresh: Boolean to determine if the full-refresh flag should be passed to the dbt run command
            no_swap: Do not swap the databases back after the run operation is complete. This will be used for a PR run
                     when the PR is opened or updated.
            drop_on_existing_db: Drop the green database if it already exists. This is used when the PR is updated and
                                 the green database needs to be recreated.
            fail_fast: Run DBT with the `--fail-fast` option. This will stop the run on the first failure.
            dbt_target: The DBT target to run the operation on. Optional, will use default if not defined.
            stomp_on_green: If set, the script will test if the green DB exists, wait 10 min, then drop the green
            database if whatever process created it is not complete.

        Returns:
            None
        """
        self.logger.info(f'Starting DBT Blue Green Swap for {self.blue_database} to {self.green_database}')
        cdb = CloneDB(self.blue_database, self.green_database, self._thread_count, query_tag=query_tag)
        # Check if the green database exists and fail if it does
        database_exists = self._check_if_database_exists(self.green_database)

        if stomp_on_green and database_exists:
            self.logger.info(
                f'Green database {self.green_database} exists. Waiting {self._stomp_on_green_timeout} minutes before dropping the database.')
            for i in range(int(self._stomp_on_green_timeout)):
                self.logger.info(f'Waiting {i} minutes')
                time.sleep(60)
                database_exists = self._check_if_database_exists(self.green_database)
                if not database_exists:
                    break
            if database_exists:
                self.logger.info(f'Green database {self.green_database} still exists. Dropping the database.')
                cdb.drop_database()
                database_exists = False

        elif database_exists and not drop_on_existing_db:
            raise Exception(f'green database {self.green_database} already exists, and script is set to fail on '
                            f'existing database. \n'
                            f'Please drop the database or set `drop_on_existing_db=True` to drop the database.')
        elif database_exists and drop_on_existing_db:
            # Drop existing database in prep for clone.
            cdb.drop_database()

        try:
            # Clone the blue (production) database to the green (temp build) database
            cdb.clone_blue_db_to_green()

            manifest = False if os.environ.get('MANIFEST_FOUND', 'false') == 'false' else True
            self.logger.info(f'Manifest Found: {manifest}')

            # Execute DBT Operations
            self._run_dbt(do_snapshot=do_snapshot, do_seed=do_seed, do_run=do_run, do_test=do_test,
                          snapshot_select=snapshot_select, snapshot_exclude=snapshot_exclude, seed_select=seed_select,
                          seed_exclude=seed_exclude, run_select=run_select,
                          run_exclude=run_exclude, test_select=test_select, test_exclude=test_exclude,
                          full_refresh=full_refresh, thread_count=self._thread_count, manifest=manifest,
                          fail_fast=fail_fast, dbt_target=dbt_target)

            # Grant usage to the green database
            self.logger.info('Granting usage to green database')
            self._grant_prd_usage()

            if not no_swap:
                # Swap the green database with the blue database
                self.logger.info(f'Swapping databases {self.blue_database} with {self.green_database}')
                self._swap_database()

                # Drop green DB
                self.logger.info('Dropping green database')
                cdb.drop_database()

        except Exception as e:
            self._swap_database_if_failure()
            # In the event of an error, drop the green database. If not dropped, the next run will fail.
            cdb.drop_database()
            raise e

        # Final check to ensure that the production database exists and hasn't somehow been removed in the process.
        # This is for debugging purposes.
        if not self._check_if_database_exists(self.blue_database):
            raise Exception(f'Green database {self.green_database} does not exist at end of B/G run! \n')

    @staticmethod
    def snowflake_connection():
        con = snowflake.connector.connect(
            account=os.environ.get('DATACOVES__MAIN__ACCOUNT'),
            warehouse=os.environ.get('DATACOVES__MAIN__WAREHOUSE'),
            database=os.environ.get('DATACOVES__MAIN__DATABASE'),
            role=os.environ.get('DATACOVES__MAIN__ROLE'),
            schema=os.environ.get('DATACOVES__MAIN__SCHEMA'),
            user=os.environ.get('DATACOVES__MAIN__USER'),
            password=os.environ.get('DATACOVES__MAIN__PASSWORD'),
            session_parameters={
                'QUERY_TAG': 'blue_green_swap',
            }
        )

        return con

    def _check_if_database_exists(self, database):
        """
        Check if the green database exists and fail if it does

        Returns:
            None
        """
        query = f"SHOW DATABASES LIKE '{database}'"
        cursor = self.con.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        if result is not None:
            return True
        else:
            return False

    def _run_dbt(self, do_snapshot: bool, do_seed: bool, do_run: bool, do_test: bool, snapshot_select: str,
                 snapshot_exclude: str, seed_select: str, seed_exclude: str, run_select: str, run_exclude: str,
                 test_select: str, test_exclude: str,
                 full_refresh: bool, thread_count: int, manifest: bool, fail_fast: bool, dbt_target: str = None):
        """
        Run DBT commands

        Args:
            do_snapshot: Boolean to determine if the snapshot command should be run
            do_seed: Boolean to determine if the seed command should be run
            do_run: Boolean to determine if the run command should be run
            do_test: Boolean to determine if the test command should be run
            snapshot_select: The models or tags to include in the snapshot command `--select`
            snapshot_exclude: The models or tags to exclude in the snapshot command `--exclude`
            seed_select: The models or tags to include in the seed command `--select`
            seed_exclude: The models or tags to exclude in the seed command `--exclude`
            seed_select: The models or tags to include in the seed command `--select`
            seed_exclude: The models or tags to exclude in the seed command `--exclude`
            run_select: The models or tags to include in the run command `--select`
            run_exclude: The models or tags to exclude in the run command `--exclude`
            test_select: The models or tags to include in the test command `--select`
            test_exclude: The models or tags to exclude in the test command `--exclude`
            full_refresh: Boolean to determine if the full-refresh flag should be passed to the dbt run command
            thread_count: The number of threads to pass to the --threads dbt flag
            manifest: Boolean to determine if the manifest was located and a state based run can be executed. If found,
                      the run will execute with `--defer --state logs -s state:modified+` flags
            fail_fast: Boolean to determine if the fail-fast flag should be passed to the dbt run command
            dbt_target: The DBT target to use for the command

        Returns:
            None
        """
        # Run snapshots
        self.execute_dbt_command('deps', [])
        args = ['--threads', str(thread_count)]
        if manifest and ((do_run and not run_select) or (do_test and not test_select)):
            args = args + ['--defer', '--state', 'logs']
        if full_refresh:
            args.append('--full-refresh')
        if fail_fast:
            args.append('--fail-fast')
        if dbt_target:
            args.extend(['--target', dbt_target])

        select, exclude = self._make_select_exclude_statement(do_snapshot, do_seed, do_run, do_test,
                                                              snapshot_select, snapshot_exclude, seed_select,
                                                              seed_exclude, run_select, run_exclude, test_select,
                                                              test_exclude, manifest)

        if select:
            args.extend(['--select', select])
        if exclude:
            args.extend(['--exclude', exclude])

        self.execute_dbt_command('build', args)

    def _make_select_exclude_statement(self, do_snapshot: bool, do_seed: bool, do_run: bool, do_test: bool,
                                       snapshot_select: str, snapshot_exclude: str, seed_select: str, seed_exclude: str,
                                       run_select: str, run_exclude: str, test_select: str, test_exclude: str,
                                       manifest: bool) -> Tuple[str, str]:
        """
        Creates a single select statement for the dbt build command using resource_types: to either include, or exclude
        various dbt resource types such as seeds, data_tests, snapshots, and models.

        Args:
            do_snapshot: Boolean to determine if the snapshot command should be run
            do_seed: Boolean to determine if the seed command should be run
            do_run: Boolean to determine if the run command should be run
            do_test: Boolean to determine if the test command should be run
            snapshot_select: The models or tags to include in the snapshot command `--select`
            snapshot_exclude: The models or tags to exclude in the snapshot command `--exclude`
            run_select: The models or tags to include in the run command `--select`
            run_exclude: The models or tags to exclude in the run command `--exclude`
            test_select: The models or tags to include in the test command `--select`
            test_exclude: The models or tags to exclude in the test command `--exclude`
            manifest: Boolean to determine if the manifest was located and a state based run can be executed. If found,
                        the run will execute with `--defer --state logs -s state:modified+` flags

        Returns:
            A tuple of strings containing the select and exclude statements
        """
        snapshot_select_str = ''
        snapshot_exclude_str = ''
        seed_select_str = ''
        seed_exclude_str = ''
        run_select_str = ''
        run_exclude_str = ''
        test_select_str = ''
        test_exclude_str = ''
        if do_snapshot:
            if snapshot_select:
                snapshot_select_list = snapshot_select.split(' ')
                for snapshot in snapshot_select_list:
                    snapshot_select_str = f'{snapshot_select_str} resource_type:snapshot,{snapshot}'
                    snapshot_select_str = snapshot_select_str.strip()
            else:
                snapshot_select_str = 'resource_type:snapshot'
            if snapshot_exclude:
                snapshot_exclude_list = snapshot_exclude.split(' ')
                for snapshot_exclude in snapshot_exclude_list:
                    snapshot_exclude_str = f'{snapshot_exclude_str} resource_type:snapshot,{snapshot_exclude}'
                    snapshot_exclude_str = snapshot_exclude_str.strip()

        if not do_snapshot:
            snapshot_exclude_str = 'resource_type:snapshot'

        if do_seed:
            if seed_select:
                seed_select_list = seed_select.split(' ')
                for seed in seed_select_list:
                    seed_select_str = f'{seed_select_str} resource_type:seed,{seed}'
                    seed_select_str = seed_select_str.strip()
            else:
                seed_select_str = 'resource_type:seed'
            if seed_exclude:
                seed_exclude_list = seed_exclude.split(' ')
                for seed_exclude in seed_exclude_list:
                    seed_exclude_str = f'{seed_exclude_str} resource_type:seed,{seed_exclude}'
                    seed_exclude_str = seed_exclude_str.strip()

        if not do_seed:
            seed_exclude_str = 'resource_type:seed'

        if do_run:
            if run_select:
                run_select_list = run_select.split(' ')
                for run in run_select_list:
                    run_select_str = f'{run_select_str} resource_type:model,{run}'
                    run_select_str = run_select_str.strip()
            elif not run_select and not manifest:
                run_select_str = 'resource_type:model'
            elif not run_select and manifest:
                run_select_str += 'resource_type:model,state:modified+'

            if run_exclude:
                run_exclude_list = run_exclude.split(' ')
                for run_exclude in run_exclude_list:
                    run_exclude_str = f'{run_exclude_str} resource_type:model,{run_exclude}'
                    run_exclude_str = run_exclude_str.strip()

        if not do_run:
            run_exclude_str = 'resource_type:model'

        if do_test:
            if test_select:
                test_select_list = test_select.split(' ')
                for test in test_select_list:
                    test_select_str = f'{test_select_str} resource_type:test,{test}'
                    test_select_str = test_select_str.strip()
            elif not test_select and not manifest:
                test_select_str = 'resource_type:test'
            elif not test_select and manifest:
                test_select_str += 'resource_type:test,state:modified+'
            if test_exclude:
                test_exclude_list = test_exclude.split(' ')
                for test_exclude in test_exclude_list:
                    test_exclude_str = f'{test_exclude_str} resource_type:test,{test_exclude}'
                    test_exclude_str = test_exclude_str.strip()

        if not do_test:
            test_exclude_str = 'resource_type:test'

        select_statement = ' '.join(
            [x for x in [snapshot_select_str, seed_select_str, run_select_str, test_select_str] if x != ''])
        exclude_statement = ' '.join(
            [x for x in [snapshot_exclude_str, seed_exclude_str, run_exclude_str, test_exclude_str] if x != ''])

        return select_statement, exclude_statement

    def _grant_prd_usage(self):
        try:
            sql = f'grant usage on database {self.green_database} to role z_db_{self.blue_database.lower()};'
            self.con.cursor().execute(sql)
            sql = f'grant usage on database {self.green_database} to role useradmin;'
            self.con.cursor().execute(sql)
        except Exception as e:
            self.logger.info(f'Error granting usage to green database: {e}')
            raise e

    def _swap_database(self):
        try:
            sql = f'alter database {self.blue_database} swap with {self.green_database};'
            self.con.cursor().execute(sql)
        except Exception as e:
            self.logger.info(f'Error swapping databases: {e}')
            raise e

    def _swap_database_if_failure(self):
        try:
            error_db = f'{self.green_database}_ERROR'
            sql = f'create or replace database {error_db} clone {self.green_database};'
            self.con.cursor().execute(sql)
            sql = f'drop database if exists {self.green_database};'
            self.con.cursor().execute(sql)
        except Exception as e:
            self.logger.info(f'Error swapping databases: {e}')
            raise e

    def execute_dbt_command(self, command: str, args: List[str]):

        dbt_command = ['dbt', command] + args
        self.logger.info(f'Running command: {" ".join(dbt_command)}')
        process = subprocess.Popen(
            dbt_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,  # Ensure outputs are in text mode rather than bytes
            cwd=self._dbt_root
        )

        # Real-time output streaming
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                self.logger.info(output.strip())  # self.logger.info each line of the output

        # Capture and self.logger.info any remaining output after the loop
        stdout, stderr = process.communicate()
        if stdout:
            self.logger.info(stdout.strip())

        # Check exit code
        if process.returncode != 0:
            self.logger.info(f"Command resulted in an error: {stderr}")
            raise subprocess.CalledProcessError(returncode=process.returncode, cmd=dbt_command, output=stderr)

        # Check for errors using a regex method if necessary
        if self.contains_errors(stdout + stderr):
            return False
        return True

    @staticmethod
    def contains_errors(text):
        pattern = r"([2-9]|\d{2,})\s+errors?"
        error_flag = bool(re.search(pattern, text))
        return error_flag
