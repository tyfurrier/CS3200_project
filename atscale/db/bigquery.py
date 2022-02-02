import logging
import os

import pandas
import pandas as pd

from .database import Database


class BigQuery(Database):
    """
        An object used for all interaction between AtScale and Google BigQuery as well as storage of all necessary
        information for the connected BigQuery database
        """

    def __init__(self, atscale_connection_id, credentials_path, project, dataset):
        """ Creates a database connection to allow for writeback to a BigQuery warehouse.

        :param str atscale_connection_id: The connection name for the warehouse in AtScale.
        :param str credentials_path: The path to the JSON file with the database credentials.
        :param str project: The database project.
        :param str dataset: The database dataset.
        """
        try:
            from sqlalchemy import create_engine
        except ImportError as e:
            from atscale.errors import AtScaleExtrasDependencyImportError
            raise AtScaleExtrasDependencyImportError('gbq', str(e))
        for parameter in [atscale_connection_id, credentials_path, project, dataset]:
            if not parameter:
                raise Exception('One or more of the given parameters are None or Null, all must be included to create'
                                'a connection')
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        engine = create_engine(f'bigquery://{project}/{dataset}?credentials_path={credentials_path}')
        connection = engine.connect()
        connection.close()

        self.database_name = project
        self.schema = dataset
        self.atscale_connection_id = atscale_connection_id
        self.connection_string = str(engine.url)

        logging.info('BigQuery connection created')

    def add_table(self, table_name: str, dataframe: pandas.DataFrame, chunksize: int=10000, if_exists: str='fail'):
        """ Creates a table in Google BigQuery using a pandas DataFrame.
         Does not use chunksize parameter as gbq is better without it

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

        df = dataframe.copy()

        # Google BigQuery Specific, everything above could be abstracted into atscale.py or possibly Database class
        df.to_gbq(f'{self.database_name}.{table_name}', self.database_name, if_exists=if_exists,
                      progress_bar=False)

        logging.info(f'Table \"{table_name}\" created in Big Query with {df.size} rows and {len(df.columns)} columns')

    def submit_query(self, db_query):
        """ Submits a query to BigQuery and returns the result.

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