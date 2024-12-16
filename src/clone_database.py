#!/usr/bin/env python
import time
from typing import Optional

from snowflake.connector import connect as sf_connect
from snowflake.connector import SnowflakeConnection
import threading
import os
import argparse
import logging
from src.core import Core

class CloneDB(Core):
    """
    Class to clone a Snowflake database from one to another. This is intended to be used in a blue/green deployment and
    will clone the schemas and grants from the blue database to the green database.
    """

    def __init__(self,
                 blue_database: str,
                 green_database: str,
                 thread_count: int = 20,
                 account: Optional[str] = None,
                 warehouse: Optional[str] = None,
                 database: Optional[str] = None,
                 role: Optional[str] = None,
                 schema: Optional[str] = None,
                 user: Optional[str] = None,
                 password: Optional[str] = None,
                 query_tag: Optional[str] = None,
                 unit_test: Optional[bool] = False):
        """
        Blue/Green deployment for Snowflake databases.
        Args:
            blue_database: The current production database.
            green_database: The temporary database where the build will occur.
        """
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

        self.start_time = time.time()
        self.time_check = self.start_time
        self._list_of_schemas_to_exclude = ['INFORMATION_SCHEMA', 'ACCOUNT_USAGE', 'SECURITY', 'SNOWFLAKE', 'UTILS',
                                            'PUBLIC']



    def clone_blue_db_to_green(self):
        """
        Primary entry point to clone a blue (prod) database to a green (staging) database.

        Returns:
            None
        """
        # Create the DB
        self.time_check = time.time()
        self.logger.info(f"Cloning blue DB {self.blue_database} to green DB {self.green_database}")
        # Clone the blue DB to the green DB
        self.clone_database(self.blue_database, self.green_database)
        # self.clone_database_schemas(self.blue_database, self.green_database)
        self.logger.info(f"Cloning complete. Blue DB {self.blue_database} cloned to green DB {self.green_database}")
        self.logger.info(f'Clone process took {time.time() - self.start_time} seconds.')

    def drop_database(self):
        """
        Utility function to drop the green database. This is used by the primary build script when the --step_on_green
        flag is set to True.

        Returns:
            None
        """
        self.logger.info(f"Dropping green DB: {self.green_database}")
        self.con.cursor().execute(f"drop database if exists {self.green_database};")

    def clone_database(self, blue_database: str, green_database: str):
        """
        Clones the the blue database to the green database

        Args:
            green_database: The name of the green database (staging).
            blue_database: The name of the blue database (prod)

        Returns:
            None
        """
        clone_sql =  f"create database {green_database} clone {blue_database};"
        self.con.cursor().execute(clone_sql)

if __name__ == "__main__":
    '''
    This section is really only designed for testing purposes. When used in production, it's is intended that you will 
    call the clone_blue_db_to_green method from an external script or directly from the DAG as needed. 
    '''
    parser = argparse.ArgumentParser(
        description="Script to run a blue/green swap")

    # Add the arguments
    parser.add_argument('--blue-db', type=str, default=os.environ.get('DATACOVES__MAIN__DATABASE'),
                        help='The source database.')
    parser.add_argument('--green-db', type=str, help='The name of the green (temporary build) database.')

    # Parse the arguments
    args = parser.parse_args()

    # Handle the case when --green-db is not provided
    if args.green_db is None:
        args.green_db = f'{args.blue_db}_STAGING'

    blue_db = args.blue_db
    green_db = args.green_db

    c = CloneDB(blue_db, green_db)
    c.clone_blue_db_to_green()
