from abc import ABC, abstractmethod
import pandas


class Database(ABC):
    """
    Database is an object used for all interaction between AtScale and the supported database
    """

    @abstractmethod
    def add_table(self, table_name: str, dataframe: pandas.DataFrame, chunksize: int=None, if_exists: str='fail'):
        """ Creates a table in the database and inserts a DataFrame into the table.
        :param str table_name: What the table should be named.
        :param pandas.DataFrame dataframe: The DataFrame to upload to the table.
        :param int chunksize: the number of rows to insert at a time. Defaults to 10,000. If None, uses default
        :param string if_exists: what to do if the table exists. Valid inputs are 'append', 'replace',
        and 'fail'. Defaults to 'fail'.
        :raises UserError if chunksize is set to a value less than 1
        :raises Exception if 'if_exists' is not one of ['append', 'replace', 'fail']
        """
        pass

    @abstractmethod
    def submit_query(self, db_query):
        """ Submits a query to the database and returns the result.

        :param str db_query: The query to submit to the database.
        :return: The queried data.
        :rtype: pandas.DataFrame
        """

    @abstractmethod
    def get_atscale_connection_id(self) -> str:
        """Returns the atscale_connection_id attribute"""

    @abstractmethod
    def get_schema(self) -> str:
        """Returns an empty string if schema does not exist in the database type"""

    @abstractmethod
    def get_database_name(self) -> str:
        """Returns an empty string if database name is not an attribute in the database type"""

    @abstractmethod
    def fix_table_name(self, table_name: str) -> str:
        """Returns an all caps or all lowercase version of the given name if the database requires"""
        return table_name