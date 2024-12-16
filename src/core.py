import time
from typing import Optional

from snowflake.connector import connect as sf_connect
from snowflake.connector import SnowflakeConnection
import threading
import os
import argparse
import logging

class Core:

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
        self.logger = logging.getLogger(__name__)
        self.unit_test = unit_test
        self.start_time = time.time()
        self.time_check = self.start_time
        self._list_of_schemas_to_exclude = ['INFORMATION_SCHEMA', 'ACCOUNT_USAGE', 'SECURITY', 'SNOWFLAKE', 'UTILS',
                                            'PUBLIC']
        if not self.unit_test:
            account=os.environ.get('DATACOVES__MAIN__ACCOUNT', account),
            warehouse=os.environ.get('DATACOVES__MAIN__WAREHOUSE', warehouse),
            database=os.environ.get('DATACOVES__MAIN__DATABASE', database),
            role=os.environ.get('DATACOVES__MAIN__ROLE', role),
            schema=os.environ.get('DATACOVES__MAIN__SCHEMA', schema),
            user=os.environ.get('DATACOVES__MAIN__USER', user),
            password=os.environ.get('DATACOVES__MAIN__PASSWORD', password),
            query_tag = 'blue_green_tag_not_set' if not query_tag else f'{query_tag}_blue_green'
            self.con = self.snowflake_connection(query_tag=query_tag,
                                                 account=account,
                                                 warehouse=warehouse,
                                                 database=database,
                                                 role=role,
                                                 schema=schema,
                                                 user=user,
                                                 password=password)
        if blue_database is None:
            self.blue_database = os.environ.get('DATACOVES__MAIN__DATABASE', None)
            if self.blue_database is None:
                raise Exception('Blue database not provided and not found in environment variables.')
        else:
            self.blue_database = blue_database
        if green_database is None:
            self.green_database = self.blue_database + '_STAGING'
        else:
            self.green_database = green_database
        self._thread_count = thread_count

    @staticmethod
    def snowflake_connection(query_tag: str,
                             account: str,
                             warehouse: str,
                             database: str,
                             role: str,
                             schema: str,
                             user: str,
                             password: str) -> SnowflakeConnection:
        """
        Create a connection to Snowflake.
        Args:
            query_tag: A value to help identify the query in the Snowflake monitoring system.
            account: Snowflake account name formatted like `pzahane-xm10992` (note, hyphen, not underscore)
            warehouse: The name of the warehouse to use for the connection
            database: The name of the database to use for the connection
            role: The role to use for the connection
            schema: The schema to use for the connection. Note, this doesn't really matter.
            user: The user to use for the connection
            password: The password to use for the connection

        Returns:
            A snowflake connection
        """
        con = sf_connect(
            account=account,
            warehouse=warehouse,
            database=database,
            role=role,
            schema=schema,
            user=user,
            password=password,
            session_parameters={
                'QUERY_TAG': query_tag,
            }
        )
        return con
