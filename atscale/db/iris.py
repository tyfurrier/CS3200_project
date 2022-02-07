import getpass
import logging

import pandas
import pandas as pd

from database import Database


class Iris(Database):
    """An object used for all interaction between AtScale and Iris as well as storage of all necessary
                information for the connected Iris database"""

    def __init__(self, atscale_connection_id, username, host, namespace, driver, schema, port=1972):
        """ Creates a database connection to allow for writeback to a IRIS warehouse.

        :param str atscale_connection_id: The connection name for the warehouse in AtScale.
        :param str username: The database username.
        :param str host: The host.
        :param str namespace: The namespace name.
        :param str driver: The IRIS driver to use.
        :param str schema: The database schema.
        :param str port: The database port (defaults to 1972).
        """
        try:
            import pyodbc as po
        except ImportError as e:
            from atscale.errors import AtScaleExtrasDependencyImportError
            raise AtScaleExtrasDependencyImportError('iris', str(e))

        for parameter in [atscale_connection_id, username, host, namespace, driver, schema, port]:
            if not parameter:
                raise Exception('One or more of the given parameters are None or Null, all must be included to create'
                                'a connection')
        password = getpass.getpass(prompt='Password: ')

        connection_string = f'DRIVER={driver};SERVER={host};PORT={port};DATABASE={namespace};UID={username};PWD={password}'
        connection = po.connect(connection_string, autocommit=True)
        connection.close()

        self.namespace = namespace
        self.schema = schema
        self.atscale_connection_id = atscale_connection_id
        self.connection_string = connection_string

        logging.info('Iris db connection created')

    def add_table(self, table_name: str, dataframe: pandas.DataFrame, chunksize: int=250, if_exists: str='fail'):
        """ Creates a table in Iris using a pandas DataFrame.

                        :param str table_name: The table to insert into.
                        :param pandas.DataFrame dataframe: The DataFrame to upload to the table.
                        :param int chunksize: the number of rows to insert at a time. Defaults to None to use default value for database.
                        :param string if_exists: what to do if the table exists. Valid inputs are 'append', 'replace', and 'fail'. Defaults to 'fail'.
                        """
        import pyodbc as po
        if_exists = if_exists.lower()
        if if_exists not in ['append', 'replace', 'fail']:
            raise Exception(f'Invalid value for parameter \'if_exists\': {if_exists}. '
                            f'Valid values are \'append\', \'replace\', and \'fail\'')

        df = dataframe.copy()

        if chunksize is None:
            chunksize=250
        if int(chunksize) < 1:
            from atscale.errors import UserError
            raise UserError('Chunksize must be greater than 0, or not passed as a parameter to use default value')

        conversion_dict_iris = {
            '<class \'numpy.int32\'>': 'INTEGER',
            '<class \'numpy.int64\'>': 'DOUBLE',
            '<class \'numpy.float64\'>': 'FLOAT',
            '<class \'str\'>': 'VARCHAR(4096)',
            '<class \'numpy.bool_\'>': 'BIT',
            '<class \'pandas._libs.tslibs.timestamps.Timestamp\'>': 'DATETIME',
            '<class \'datetime.date\'>': 'DATE',
            '<class \'decimal.Decimal\'>': 'DECIMAL'
        }

        connection = po.connect(self.connection_string, autommit=True)
        cursor = connection.cursor()

        if cursor.tables(table=table_name, schema=self.schema).fetchone():
            exists = True
        else:
            exists = False

        if exists and if_exists == 'fail':
            raise Exception(f'A table named: {table_name} already exists in schema: {self.schema}')

        if exists and if_exists == 'replace':
            operation = f"DROP TABLE \"{self.schema}\".\"{table_name}\""
            cursor.execute(operation)
            logging.debug(f'{table_name} already exists in the Iris db, so it was replaced')

        types = {}
        for i in dataframe.columns:
            if str(type(dataframe[i].loc[~dataframe[i].isnull()].iloc[0])) in conversion_dict_iris:
                types[i] = conversion_dict_iris[str(type(dataframe[i].loc[~dataframe[i].isnull()].iloc[0]))]
            else:
                types[i] = conversion_dict_iris['<class \'str\'>']

        if not cursor.tables(table=table_name, tableType='TABLE').fetchone():
            operation = "CREATE TABLE \"{}\".\"{}\" (".format(self.schema, table_name)
            for key, value in types.items():
                operation += "\"{}\" {}, ".format(key, value)
            operation = operation[:-2]
            operation += ")"
            cursor.execute(operation)

        operation = "INSERT INTO \"{}\".\"{}\" (".format(self.schema, table_name)
        for col in dataframe.columns:
            operation += "\"{}\", ".format(col)
        operation = operation[:-2]
        operation += ") "

        list_df = [dataframe[i:i + chunksize] for i in range(0, dataframe.shape[0], chunksize)]
        for df in list_df:
            op_copy = operation
            for index, row in df.iterrows():
                op_copy += 'SELECT '
                for cl in df.columns:
                    op_copy += "'{}', ".format(row[cl])
                op_copy = op_copy[:-2]
                op_copy += " UNION ALL "
            op_copy = op_copy[:-11]
            cursor.execute(op_copy)
        connection.close()

        logging.info(f'Table \"{table_name}\" created in Databricks with {df.size} rows and {len(df.columns)} columns')

    def submit_query(self, db_query):
        """ Submits a query to Snowflake and returns the result.

        :param str db_query: The query to submit to the database.
        :return: The queried data.
        :rtype: pandas.DataFrame
        """
        import pyodbc as po
        engine = po.connect(self.connection_string, autocommit=True)
        connection = engine.connect()
        df = pd.read_sql_query(db_query, connection)
        return df

    def get_atscale_connection_id(self):
        return self.atscale_connection_id

    def get_database_name(self):
        """ Returns the namespace attribute as that is Iris's equivalent to database """
        return self.namespace

    def get_schema(self):
        return self.schema
