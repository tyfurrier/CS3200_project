import getpass
import logging

import pandas as pd

from database import Database


class Snowflake(Database):
    """
    An object used for all interaction between AtScale and Snowflake as well as storage of all necessary information
    for the connected Snowflake
    """
    def __init__(self, atscale_connection_id, username, account, warehouse, database, schema):
        """ Creates a database connection to allow for writeback to a Snowflake warehouse.

        :param str username: The database username.
        :param str account: The database account.
        :param str warehouse: The database warehouse.
        :param str database: The database name.
        :param str schema: The database schema.
        """
        try:
            from sqlalchemy import create_engine
        except ImportError as e:
            from atscale.errors import AtScaleExtrasDependencyImportError
            raise AtScaleExtrasDependencyImportError('snowflake', str(e))

        for parameter in [atscale_connection_id, username, account, warehouse, database, schema]:
            if not parameter:
                raise Exception('One or more of the given parameters are None or Null, all must be included to create'
                                'a connection')
        password = getpass.getpass(prompt='Password: ')

        engine = create_engine(
            f'snowflake://{username}:{password}@{account}/{database}/{schema}?warehouse={warehouse}')
        connection = engine.connect()
        connection.close()

        self.database_name = database
        self.schema = schema
        self.atscale_connection_id = atscale_connection_id
        self.connection_string = str(engine.url)

        logging.info('Snowflake db connection created')

    def add_table(self, table_name, dataframe, chunksize=10000, if_exists='fail'):
        """ Inserts a DataFrame into table.

        :param str table_name: The table to insert into.
        :param pandas.DataFrame dataframe: The DataFrame to upload to the table.
        :param int chunksize: the number of rows to insert at a time. Defaults to None to use default value for database.
        :param string if_exists: what to do if the table exists. Valid inputs are 'append', 'replace', and 'fail'. Defaults to 'fail'.
        """
        from sqlalchemy import create_engine
        if_exists = if_exists.lower()
        if if_exists not in ['append', 'replace', 'fail']:
            raise Exception(f'Invalid value for parameter \'if_exists\': {if_exists}. '
                            f'Valid values are \'append\', \'replace\', and \'fail\'')
        if chunksize is None:
            chunksize=10000
        if int(chunksize) < 1:
            raise Exception('Chunksize must be greater than 0 or not passed in to use default value')

        df = dataframe.copy()

        # Snowflake Specific, everything above could be abstracted into atscale.py or possibly Database class

        df.columns = df.columns.str.upper()
        table_name = table_name.upper()

        engine = create_engine(self.connection_string)
        df.to_sql(name=table_name, con=engine, schema=self.schema, method='multi', index=False,
                  chunksize=chunksize, if_exists=if_exists)
        logging.info(f'Table \"{table_name}\" created in Snowflake '
                     f'with {df.size} rows and {len(df.columns)} columns \n using chunksize {chunksize}')

    def submit_query(self, db_query):
        """ Submits a query to Snowflake and returns the result.

        :param str db_query: The query to submit to the database.
        :return: The queried data.
        :rtype: pandas.DataFrame
        """
        from sqlalchemy import create_engine
        engine = create_engine(self.connection_string)
        connection = engine.connect()
        df = pd.read_sql_query(db_query, connection)
        return df

    def get_atscale_connection_id(self):
        return self.atscale_connection_id

    def get_database_name(self) -> str:
        return self.database_name

    def get_schema(self) -> str:
        return self.schema

    def fix_table_name(self, table_name: str) -> str:
        """Returns table_name.upper()"""
        return table_name.upper()