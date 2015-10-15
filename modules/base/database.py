# Standard Python Modules
import logging
import os.path

from datetime import datetime, timedelta

# ETL Code
from   modules.base.jdbc            import JDBC
from   modules.base.etl_base       import EtlBase
from   modules.base.constants       import *

class Database(EtlBase):
    '''
    Base class for all Database Classes.
    Will be removed if there is nothing added to this class shortly.
    '''
    def __init__(self):
        super(Database, self).__init__()

    #####################
    # Private Functions #
    #####################

    ####################
    # Public Functions #
    ####################
    def getPrimaryKey(self, database, table):
        '''
        Get the Primary Key for a given table
        Returns None, No Primary Key exists for the given table
        Returns Array, Column Names for the primary key columns
        '''
        return None

    def getDatabaseInfo(self , database):
        '''
        Gets the username, host, schema, etc
        '''
        dbInfo = {'username': None, 'host': None, 'port': None, 'schema': None}
        return dbInfo

    def getSchemaList(self, filterSysSchemas=True):
        '''
        Returns the schemas that exist within the given database
        '''
        return None

    def getTableDefinition(self, database, tablename):
        '''
        Returns table definition
        '''
        return None


