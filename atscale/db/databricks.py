import logging

import pandas
import pandas as pd
from .database import Database


class Databricks(Database):
    """An object used for all interaction between AtScale and Databricks as well as storage of all necessary
            information for the connected Databricks database"""

    def __init__(self, atscale_connection_id, token, host, schema, http_path, driver, port=443):
        """ Creates a database connection to allow for writeback to a Databricks warehouse.

        :param str atscale_connection_id: The connection name for the warehouse in AtScale.
        :param str token: The database token.
        :param str host: The host.
        :param str database: The database name.
        :param str http_path: The database HTTP path.
        :param str driver: The Databricks driver to use.
        :param str port: The database port (defaults to 443).
        """
        try:
            from sqlalchemy import create_engine
        except ImportError as e:
            from atscale.errors import AtScaleExtrasDependencyImportError
            raise AtScaleExtrasDependencyImportError('databricks', str(e))

        for parameter in [atscale_connection_id, token, host, schema, http_path, driver, port]:
            if not parameter:
                raise Exception('One or more of the given parameters are None or Null, all must be included to create'
                                'a connection')
        engine = create_engine(f'databricks+pyodbc://token:{token}@{host}:{port}/{schema}',
                               connect_args={'http_path': http_path, 'driver_path': driver})
        connection = engine.connect()
        connection.close()

        self.schema = schema
        self.atscale_connection_id = atscale_connection_id
        self.connection_string = str(engine.url)
        self.http_path = http_path
        self.driver = driver
        logging.info('Databricks database created')

    def add_table(self, table_name: str, dataframe: pandas.DataFrame, chunksize: int=10000, if_exists: str='fail'):
        """ Creates a table in Databricks using a pandas DataFrame.

                :param str table_name: The table to insert into.
                :param pandas.DataFrame dataframe: The DataFrame to upload to the table.
                :param int chunksize: the number of rows to insert at a time. Defaults to None to use default value for database.
                :param string if_exists: what to do if the table exists. Valid inputs are 'append', 'replace', and 'fail'. Defaults to 'fail'.
                """
        if_exists = if_exists.lower()
        if if_exists not in ['append', 'replace', 'fail']:
            raise Exception(f'Invalid value for parameter \'if_exists\': {if_exists}. '
                            f'Valid values are \'append\', \'replace\', and \'fail\'')

        df = dataframe.copy()

        if chunksize is None:
            chunksize=10000
        if int(chunksize) < 1:
            from atscale.errors import UserError
            raise UserError('Chunksize must be greater than 0, or not passed as a parameter to use default value')

        conversion_dict_databricks = {
            '<class \'numpy.int64\'>': 'Integer',
            '<class \'numpy.float64\'>': 'Float',
            '<class \'str\'>': 'String',
            '<class \'numpy.bool_\'>': 'Boolean',
            '<class \'pandas._libs.tslibs.timestamps.Timestamp\'>': 'Timestamp',
            '<class \'datetime.date\'>': 'Date',
            '<class \'decimal.Decimal\'>': 'Decimal'
        }
        engine = create_engine(self.connection_string,
                               connect_args={'http_path': self.http_path, 'driver_path': self.driver})

        exists = engine.has_table(table_name, schema=self.schema)
        if exists and if_exists == 'fail':
            raise Exception(f'A table named: {table_name} in schema: {self.schema} already exists')

        connection = engine.raw_connection()
        cursor = connection.cursor()

        if exists and if_exists == 'replace':
            operation = f'DROP TABLE IF EXISTS `{self.schema}`.`{table_name}`'
            cursor.execute(operation)

        types = {}
        for i in dataframe.columns:
            if str(type(dataframe[i].loc[~dataframe[i].isnull()].iloc[0])) in conversion_dict_databricks:
                types[i] = conversion_dict_databricks[str(type(dataframe[i].loc[~dataframe[i].isnull()].iloc[0]))]
            else:
                types[i] = conversion_dict_databricks['<class \'str\'>']

        operation = f'CREATE TABLE IF NOT EXISTS `{self.schema}`.`{table_name}` ('
        for key, value in types.items():
            operation += f'`{key}` {value}, '
        operation = operation[:-2]
        operation += ")"
        cursor.execute(operation)

        operation = f"INSERT INTO `{self.schema}`.`{table_name}` VALUES ("

        list_df = [dataframe[i:i + chunksize] for i in range(0, dataframe.shape[0], chunksize)]
        for df in list_df:
            op_copy = operation
            for index, row in df.iterrows():
                for col in df.columns:
                    if 'String' in types[col]:
                        op_copy += "'{}', ".format(row[col])
                    elif 'Date' in types[col] or 'Timestamp' in types[col]:
                        op_copy += "cast('{}' as {}), ".format(row[col], types[col])
                    else:
                        op_copy += f"{row[col]}, "
                op_copy = op_copy[:-2]
                op_copy += "), ("
            op_copy = op_copy[:-3]
            cursor.execute(op_copy)
        connection.close()

        logging.info(f'Table \"{table_name}\" created in Databricks with {df.size} rows and {len(df.columns)} columns')

    def submit_query(self, db_query):
        """ Submits a query to Snowflake and returns the result.

        :param str db_query: The query to submit to the database.
        :return: The queried data.
        :rtype: pandas.DataFrame
        """
        engine = create_engine(self.connection_string, connect_args={'http_path': self.http_path,
                                                                     'driver_path': self.driver})
        connection = engine.connect()
        df = pd.read_sql_query(db_query, connection)
        return df

    def get_atscale_connection_id(self):
        return self.atscale_connection_id

    def get_database_name(self) -> str:

        """ Returns '' since databricks has no database equivalent
        """
        return ''

    def get_schema(self) -> str:
        return self.schema