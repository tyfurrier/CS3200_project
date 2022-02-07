import getpass
import logging
import pandas as pd
from database import Database


class Synapse(Database):
    """An object used for all interaction between AtScale and Synapse as well as storage of all necessary
            information for the connected Synapse database"""

    def __init__(self, atscale_connection_id, username, host, database, driver, schema, port=1433):
        """ Creates a database connection to allow for writeback to a Synapse warehouse.

        :param str atscale_connection_id: The connection name for the warehouse in AtScale.
        :param str username: The database username.
        :param str host: The host.
        :param str database: The database name.
        :param str driver: The SQL server driver to use.
        :param str schema: The database schema.
        :param str port: The database port (defaults to 1433).
        """
        try:
            import pyodbc as po
        except ImportError as e:
            from atscale.errors import AtScaleExtrasDependencyImportError
            raise AtScaleExtrasDependencyImportError('synapse', str(e))

        for parameter in [atscale_connection_id, username, host, database, driver, schema, port]:
            if not parameter:
                raise Exception('One or more of the given parameters are None or Null, all must be included to create'
                                'a connection')
        password = getpass.getpass(prompt='Password: ')

        connection_string = f'DRIVER={driver};SERVER={host};PORT={port};DATABASE={database};UID={username};PWD={password}'
        connection = po.connect(connection_string, autocommit=True)
        #engine = create_engine(f'mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver={urllib.parse.quote_plus(driver)}'
        # connection = engine.connect()
        connection.close()

        self.database_name = database
        self.schema = schema
        self.atscale_connection_id = atscale_connection_id
        self.connection_string = connection_string

        logging.info('Synapse db connection created')

    def add_table(self, table_name: str, dataframe: pd.DataFrame, chunksize: int=10000, if_exists: str='fail'):
        """ Creates a table in synapse using a pandas DataFrame.

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
            chunksize=10000
        if int(chunksize) < 1:
            from atscale.errors import UserError
            raise UserError('Chunksize must be greater than 0, or not passed as a parameter to use default value')

        #Synapse specific from here on

        conversion_dict_synapse = {
            '<class \'numpy.int64\'>': 'int',
            '<class \'numpy.float64\'>': 'real',
            '<class \'str\'>': 'nvarchar(4000)',
            '<class \'numpy.bool_\'>': 'bit',
            '<class \'pandas._libs.tslibs.timestamps.Timestamp\'>': 'datetime',
            '<class \'datetime.date\'>': 'date',
        }

        connection = po.connect(self.connection_string, autocommit=True)
        cursor = connection.cursor()

        if cursor.tables(table=table_name, schema=self.schema).fetchone():
            exists = True
        else:
            exists = False

        if exists and if_exists == 'fail':
            raise Exception(f'A table named: {table_name} in schema: {self.schema} already exists')

        if exists and if_exists == 'replace':
            operation = f"DROP TABLE \"{self.schema}\".\"{table_name}\""
            cursor.execute(operation)

        types = {}
        for i in dataframe.columns:
            if str(type(dataframe[i].loc[~dataframe[i].isnull()].iloc[0])) in conversion_dict_synapse:
                types[i] = conversion_dict_synapse[str(type(dataframe[i].loc[~dataframe[i].isnull()].iloc[0]))]
            else:
                types[i] = conversion_dict_synapse['<class \'str\'>']

        if not cursor.tables(table=table_name, tableType='TABLE').fetchone():
            operation = f"CREATE TABLE \"{self.schema}\".\"{table_name}\" ("
            for key, value in types.items():
                operation += f"{key} {value}, "
            operation = operation[:-2]
            operation += ")"
            cursor.execute(operation)

        operation = f"INSERT INTO \"{self.schema}\".\"{table_name}\" ("
        for col in dataframe.columns:
            operation += f"{col}, "
        operation = operation[:-2]
        operation += ") "

        list_df = [dataframe[i:i + chunksize] for i in range(0, dataframe.shape[0], chunksize)]
        for df in list_df:
            op_copy = operation
            for index, row in df.iterrows():
                op_copy += 'SELECT '
                for cl in df.columns:
                    if 'nvarchar' in types[cl] or 'date' in types[cl]:
                        op_copy += "'{}', ".format(row[cl])
                    else:
                        op_copy += "{}, ".format(row[cl])
                op_copy = op_copy[:-2]
                op_copy += " UNION ALL "
            op_copy = op_copy[:-11]
            cursor.execute(op_copy)
        connection.close()

        logging.info(f'Table \"{table_name}\" created in Synapse with {df.size} rows and {len(df.columns)} columns')

    def submit_query(self, db_query):
        """ Submits a query to Synapse and returns the result.

        :param str db_query: The query to submit to the database.
        :return: The queried data.
        :rtype: pandas.DataFrame
        """
        import pyodbc as po
        connection = po.connect(self.connection_string, autocommit=True)
        df = pd.read_sql_query(db_query, connection)
        return df

    def get_atscale_connection_id(self):
        return self.atscale_connection_id

    def get_database_name(self) -> str:
        return self.database_name

    def get_schema(self) -> str:
        return self.schema