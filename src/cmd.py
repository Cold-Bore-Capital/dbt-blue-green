#!/usr/bin/env python

import argparse

from src.main import DBTBlueGreen
from src.logging_setup import setup_logging

if __name__ == "__main__":

    setup_logging()

    parser = argparse.ArgumentParser(
        description="Script to launch DBT blue green deployment.")

    parser.add_argument('--blue-db', type=str, help='Name of the production DB to clone from and swap back to (if swap is true).')
    parser.add_argument('--green-db', type=str, help='Name of staging DB to swap to during clone operation (if swap is true).')
    parser.add_argument('--snapshot-select', type=str, help='Tags or select items to select for the dbt snapshot command.')
    parser.add_argument('--snapshot-exclude', type=str, help='Tags or select items to exclude from the dbt snapshot command.')

    parser.add_argument('--run-select', type=str, help='Tags or select items to select for the dbt run command.')
    parser.add_argument('--run-exclude', type=str, help='Tags or select items to exclude from the dbt run command.')

    parser.add_argument('--seed-select', type=str, help='Tags or select items to select for the dbt seed command.')
    parser.add_argument('--seed-exclude', type=str, help='Tags or select items to exclude from the dbt seed command.')

    parser.add_argument('--test-select', type=str, help='Tags or select items to select for the dbt test command.')
    parser.add_argument('--test-exclude', type=str, help='Tags or select items to exclude from the dbt test command.')

    parser.add_argument('--do-snapshot', action='store_true', help='Execute the DBT snapshot operation.')
    parser.add_argument('--do-seed', action='store_true', help='Execute the DBT seed operation.')
    parser.add_argument('--do-run', action='store_true', help='Execute the DBT run operation.')
    parser.add_argument('--do-test', action='store_true', help='Execute the DBT test operation.')

    parser.add_argument('--query-tag', type=str, help='A name for the query to identify in the Snowflake monitoring system.')

    parser.add_argument('--full-refresh', action='store_true', help='Execute DBT with the `--full-refresh` option.')
    parser.add_argument('--fail-fast', action='store_true', help='Execute DBT with the `--fail-fast` option.')

    parser.add_argument('--no-swap', action='store_true', help='Do not swap the database back again at end'
                                                               ' of build run.')
    drop_on_existing_db_help = 'Action to take if the staging database already exists. If true, the script will drop ' \
                               'an existing database (such as a PR database that will be updated). If not set or '\
                               'false, the script will terminate if an existing DB is found.'
    parser.add_argument('--drop-on-existing-db', action='store_true', help=drop_on_existing_db_help)

    parser.add_argument('--dbt-target', type=str, help='The dbt target to use for the dbt run command.')

    # This is used for Airflow. There is a scenario where a CI run creates a green DB and the DB still exists when
    # airflow triggers. With this flag set, the system will try to proceed every minute for 10 minutes. If the
    # green DB still exists after 10 minutes, the system will drop the green DB and proceed with the blue/green swap.
    parser.add_argument('--stomp-on-green', action='store_true', help='If set, the script will test if the green DB exists, wait 10 min, then drop the green database if whatever process created it is not complete. ')

    args = parser.parse_args()

    dbt = DBTBlueGreen(blue_database=args.blue_db, green_database=args.green_db)
    print('launch_blue_green.py. Starting run.')
    dbt.main(
        snapshot_select=args.snapshot_select,
        snapshot_exclude=args.snapshot_exclude,
        seed_select=args.seed_select,
        seed_exclude=args.seed_exclude,
        run_select=args.run_select,
        run_exclude=args.run_exclude,
        test_select=args.test_select,
        test_exclude=args.test_exclude,
        query_tag=args.query_tag,
        do_seed=args.do_seed,
        do_snapshot=args.do_snapshot,
        do_run=args.do_run,
        do_test=args.do_test,
        full_refresh=args.full_refresh,
        no_swap=args.no_swap,
        drop_on_existing_db=args.drop_on_existing_db,
        fail_fast=args.fail_fast,
        dbt_target=args.dbt_target,
        stomp_on_green=args.stomp_on_green
    )
