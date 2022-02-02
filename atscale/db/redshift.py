import getpass
import logging

import pandas
import pandas as pd


from .database import Database

class Redshift(Database):
    """
    An object used for all interaction between AtScale and Redshift as well as storage of all necessary information
    for the connected Redshift
    """
    def __init__(self, atscale_connection_id, username, host, database, schema, port='5439'):
        """ Creates a database connection to allow for writeback to a Redshift warehouse.

        :param str atscale_connection_id: The connection name for the warehouse in AtScale.
        :param str username: The database username.
        :param str host: The host.
        :param str database: The database name.
        :param str schema: The database schema.
        :param str port: The database port (defaults to 5439).
        :raises Exception if any of the inputs are of type None
        """
        try:
            from sqlalchemy import create_engine
        except ImportError as e:
            from atscale.errors import AtScaleExtrasDependencyImportError
            raise AtScaleExtrasDependencyImportError('redshift', str(e))

        for parameter in [atscale_connection_id, username, host, database, schema, port]:
            if not parameter:
                raise Exception('One or more of the given parameters are None or Null, all must be included to create'
                                'a connection')
        password = getpass.getpass(prompt='Password: ')

        engine = create_engine(f'redshift+psycopg2://{username}:{password}@{host}:{port}/{database}',
                               executemany_mode='batch', executemany_values_page_size=10000,
                               executemany_batch_page_size=500)
        connection = engine.connect()
        connection.close()

        self.database_name = database
        self.schema = schema
        self.atscale_connection_id = atscale_connection_id
        self.connection_string = str(engine.url)

        logging.info('Redshift connection created')

    def add_table(self, table_name: str, dataframe: pandas.DataFrame, chunksize: int=1000, if_exists: str='fail'):
        """ Creates a table in redshift using a pandas DataFrame.

        :param str table_name: The table to insert into.
        :param pandas.DataFrame dataframe: The DataFrame to upload to the table.
        :param int chunksize: the number of rows to insert at a time. Defaults to None to use default value for database
        :param string if_exists: what to do if the table exists. Valid inputs are 'append', 'replace', and 'fail'.
        Defaults to 'fail'.
        """
        if_exists = if_exists.lower()
        if if_exists not in ['append', 'replace', 'fail']:
            raise Exception(f'Invalid value for parameter \'if_exists\': {if_exists}. '
                            f'Valid values are \'append\', \'replace\', and \'fail\'')
        if chunksize is None:
            chunksize=1000
        if int(chunksize) < 1:
            from atscale.utils import UserError
            raise UserError('Chunksize must be greater than 0 or not passed in to use default value')

        df = dataframe.copy()

        # Redshift Specific, everything above could be abstracted into atscale.py or possibly Database class

        df.columns = df.columns.str.lower()
        table_name = table_name.lower()

        engine = create_engine(self.connection_string, executemany_mode='batch',
                               executemany_values_page_size=chunksize, executemany_batch_page_size=500)
        df.to_sql(name=table_name, con=engine, schema=self.schema, method='multi', index=False,
                  chunksize=chunksize, if_exists=if_exists)
        logging.info(f'Table \"{table_name}\" created in Redshift with {df.size} rows and {len(df.columns)} columns')

    def submit_query(self, db_query):
        """ Submits a query to Redshift and returns the result.

        :param str db_query: The query to submit to the database.
        :return: The queried data.
        :rtype: pandas.DataFrame
        """
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
        """ Returns table_name in all lowercase characters"""
        return table_name.lower()