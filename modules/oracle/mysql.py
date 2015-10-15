# Standard Python Modules
import csv
import logging
import os.path
import shutil
import uuid

from datetime import datetime, timedelta

# ETL Code
from   modules.base.jdbc            import JDBC
from   modules.base.database        import Database
from   modules.base.constants       import *

class MySQL(Database):
    '''
    A class for all MySQL specific actions
    '''
    def __init__(self):
        super(MySQL, self).__init__()
        self.config.read('conf/modules/mysql.cfg')

    #####################
    # Private Functions #
    #####################
    def _getTargetDateList(self,target_date, days_back=0):
        """
        Gets target date string list.

        Args:
            target_date: target date (yyyyMMdd)
            days_back: days back from the target_date

        Returns:
            target date list order by asc [eg: {20121231, 20130101, 20130102}]

        Raises:
            ValueError: arguments are missing or invalid.
        """
        #if not etl_util.is_date_format(target_date):
        #    raise ValueError('target_date is invalid. [target_date=%s]' % target_date)
        if not isinstance(days_back, int):
            raise ValueError('days_back must be an integer. [days_back=%s]' % days_back)
        if days_back < 0:
            raise ValueError('days_back must be 0 and over. [days_back=%s]' % days_back)

        date_list = []
        # days_back, ..., 0
        for back_day in range(days_back, -1, -1):
            date          = datetime.strptime(target_date, '%Y%m%d')
            calc_date     = date + timedelta(days=-back_day)
            str_calc_date = calc_date.strftime('%Y-%m-%d')
            date_list.append(str_calc_date)
        return date_list

    def _extractToFile(self, dbInfo, characterSet, sql, targetFilePath, header_flag=False, appendData=False):
        """
        Extracts data using the SQL and exports a file to the remote server.

        Args:
            schema: schema name
            port: port number
            username: user name
            password: password
            sql: SQL
            targetFilePath: output file path
            header_flag: header export flag (True:Shown, False:Hide)

        Returns:
            0: Success
            1: Error
        """
        mysql_cmd = self.config.get(SECTION_OS_COMMANDS, MYSQL_COMMAND)
        if mysql_cmd.find('mysql') < 0:
            # Not included 'mysql' in the command (Prevent OS command injection)
            raise Exception('mysql command error. [command=%s]' % mysql_cmd)

        header_option = ''
        if not header_flag:
            header_option = ' --skip-column-names'

        if appendData:
           redirect = '>>'
        else:
           redirect = '>'


        # Set the command line arguments needed
        option = '--default-character-set=%s' % characterSet
        option += ' -h%s' % dbInfo['host']
        option += ' -P%s' % dbInfo['port']
        option += ' -u%s' % dbInfo['username']
        option += ' -p%s' % dbInfo['password']
        if not header_flag:
            option += ' --skip-column-names'
        option += ' %s' % dbInfo['schema']
        option += ' -r -e "%s" %s %s' % (sql, redirect, targetFilePath)

        cmd    = '%s %s' % (mysql_cmd, option)

        (subProcess, stdinFilename, stdoutFilename, stderrFilename) = self._executeOsCommand(cmd, waitTillFinished=False)
        return (subProcess, stdinFilename, stdoutFilename, stderrFilename)

    ####################
    # Public Functions #
    ####################
    def getDatabaseInfo(self , database):
        '''
        Reads Database Informaton and returns it to the user
        '''
        dbInfo = self._readDatabaseSection(database)
        del dbInfo['password']
        return dbInfo

    def getInformationSchemaColumns(self, database, schema, tablename):
        """
        """
        select = """
                 SELECT   table_catalog, table_schema, table_name, column_name, ordinal_position,
                          column_default, is_nullable, data_type, character_maximum_length, character_octet_length,
                          numeric_precision, numeric_scale, character_set_name, collation_name, column_type,
                          column_key, extra, privileges, column_comment
                 FROM     information_schema.columns
                 WHERE    table_schema='%s'
                          AND table_name='%s'
                 ORDER BY ordinal_position
                 """ % (schema, tablename)
        resultSet = self.executeSql(database, select)
        informationSchema = []
        for row in resultSet:
            info = {'table_catalog'         :row[0],  'table_schema'     :row[1], 'table_name'               :row[2],
                    'column_name'           :row[3],  'ordinal_position' :row[4], 'column_default'           :row[5],
                    'is_nullable'           :row[6],  'data_type'        :row[7],  'character_maximum_length':row[8],
                    'character_octet_length':row[9],  'numeric_precision':row[10], 'numeric_scale'           :row[11],
                    'character_set_name'    :row[12], 'collation_name'   :row[13], 'column_type'             :row[14],
                    'column_key'            :row[15], 'extra'            :row[16], 'privileges'              :row[17],
                    'column_comment'        :row[18]}
            informationSchema.append(info)
        return informationSchema

    def getTableColumns(self, database, schema, tablename):
        """
        Get the names of each column in a given table
        """
        columnData = self.getInformationSchemaColumns(database, schema, tablename)
        tableColumns = []
        for column in columnData:
            tableColumns.append(column['column_name'])
        return tableColumns

    def getTableColumnTypes(self, database, schema, tablename):
        columnData = self.getInformationSchemaColumns(database, schema, tablename)
        columnTypes = {}
        for column in columnData:
            columnTypes[column['column_name']] = column['column_type']
        return columnTypes

    def getTableDefinition(self, database, tablename):
        descSql         = 'DESC %s' % tablename
        tableDefinition = []
        resultSet       = self.executeSql(database, descSql)
        for row in resultSet:
            rowInfo = {'field':row[0], 'type':row[1],'nullable':row[2],'primary_key':row[3],'default':row[4],'extra':row[5]}
            tableDefinition.append(rowInfo)
        return tableDefinition

    def extract(self, databaseName, extractInfo, fileInfo):
        """
        Extracts a table into a flatfile.

        Args:
            databaseName    :e.g., sample_order
            extractInfo     :
            fileInfo        :
        """
        dbInfo          = self._readDatabaseSection(databaseName)

        # Convert date value
        table             = extractInfo[TABLE]
        targetDateColumn  = extractInfo[TARGET_DATE_COLUMN]
        targetDate        = extractInfo[TARGET_DATE]
        daysBack          = extractInfo[DAYS_BACK]
        mode              = extractInfo[MODE]

        characterSet       = fileInfo[CHARACTER_SET]
        targetFileFullPath = fileInfo['target_file_full_path']
        workFileFullPath   = '%s_%s_%s' % (targetFileFullPath, dbInfo['schema'], str(uuid.uuid4()))

        appendData  = fileInfo['append_data']
        tableDefinition = self.getTableDefinition(databaseName, table)

        msg = "%s data for '%s' will be extracted into the file '%s'." % (dbInfo['host'], extractInfo[TARGET_DATE], workFileFullPath)
        logging.info(msg)

        # Generate Extraction SQL
        sql = self._generateExtractionSQL(table, tableDefinition, dbInfo, extractInfo, fileInfo)

        # Execute SQL
        (subprocess,stdinFilename, stdoutFilename, stderrFilename) = self._extractToFile(dbInfo, characterSet, sql, workFileFullPath, False, appendData)
        extractInfo = {'process':subprocess, 'stdin_filename':stdinFilename, 'stdout_filename':stdoutFilename, 'stderr_filename':stderrFilename, 'table':table, 'filename':targetFileFullPath, 'work_filename':workFileFullPath}
        return extractInfo

    def _createCLOBFiles(self,databaseName,tablename,tableDefinitionSorted, dbInfo, extractInfo, fileInfo):
        """
        Create CLOB files and a targetfile

        Args:

        """
        characterSet    = fileInfo[CHARACTER_SET]
        (schema_teradata,schema_mysql)        = self._getSchemaName(databaseName)
        table_teradata  = schema_mysql+'_'+tablename

        #create temp directory which has CLOB flatfiles
        tmpDirectory    = fileInfo['target_directory']+'/'+table_teradata
        self._removeWorkDirectory(tmpDirectory)
        self._createWorkDirectory(tmpDirectory)

        targetFileFullPath = fileInfo['target_file_full_path']

        #read tmpfile
        (keyPositionList,clobPositionList)  =self._getPositions(tableDefinitionSorted)
        delimeter = DELIMETER_TAB

        rowsStr=''

        #delimeter = self._getDelimeter(fileInfo[DELIMETER])
        rows = self._readFile(targetFileFullPath, characterSet, delimeter)
        for row in rows:
            #get priValues of each row    e.g., priValues='_01_01'
            priValues=''
            for i in keyPositionList:
                priValues+=('_'+row[i])

            #For each CLOB, create a flatfile
            for i in clobPositionList:
                #from shift_jis to unicode
                clobValue   = row[i]
                clobDef     = tableDefinitionSorted[i]

                #get path of CLOB file
                name        = clobDef['field'].encode('utf8')
                filePath   ="%s/%s%s_%s.txt" % (tmpDirectory,table_teradata,priValues,name)
                #e.g.,
                #filePath   = "tmpDirectory/tableName_01_01_columnName.txt"

                #create CLOB file
                self._writeFile(filePath,clobValue,characterSet,'w')

                #set row for flatfile
                row[i]     = filePath

            rowsStr    += delimeter.join(row) + '\n'


        #delete flatfile
        self._deleteFile(targetFileFullPath)

        #create flatfile : CLOB columns have path
        self._writeFile(targetFileFullPath, rowsStr, characterSet, 'w')

    def _getPositions(self, tableDefinition):
        """
        Get Clomun Position numbers

        Args:
            tableDefinition

        Returns:
            keyPositionList    :Clomun Position numbers for PRI Keys
            clobPositionList  :Clomun Position numbers for CLOB Columns
        """
        keyPositionList     =[]
        clobPositionList    =[]

        position=0
        for definition in tableDefinition:
            columnName  = definition['field'].encode('utf8')
            type        = definition['type'].encode('utf8')
            pri         = definition['primary_key'].encode('utf8')

            if pri=='PRI':
                keyPositionList.append(position)

            blobFlg = definition.get('BLOB',False)
            clobFlg = definition.get('CLOB',False)
            if blobFlg or clobFlg:
                clobPositionList.append(position)

            position+=1

        return (keyPositionList,clobPositionList)

    def _generateSelectClauseForColumn(self, name, type, permitNull,replaceType=DEFAULT_EXTRACT_REPLACE):
        ##############################
        # Transformation Rule
        #
        #   None -> empty
        #   Tab in the value -> space
        #   LF in the value -> space
        #   CR in the value -> space
        ##############################
        name  = "\\`%s\\`" % (name)
        query = name
        if type == 'timestamp':
            query = "DATE_FORMAT(" + name + ", '%Y-%m-%d %H:%i:%s')"
        elif type.strip() == 'binary(16)':
            query = "LOWER(CONCAT_WS('-', SUBSTRING(HEX(%s),1,8), SUBSTRING(HEX(%s),9,4), SUBSTRING(HEX(%s),13,4), SUBSTRING(HEX(%s),17,4), SUBSTRING(HEX(%s),21,12)))" % (name, name, name, name, name)
        elif type == 'datetime':
            query = "DATE_FORMAT(" + name + ", '%Y-%m-%d %H:%i:%s')"
        elif type == 'date':
            query = "DATE_FORMAT(" + name + ", '%Y-%m-%d')"
        elif type.find('char') != -1:
            query = "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(%s, '\\t', ' '), '\\n', ' '), '\\r', ' '), '\\0',''), 0x7F, ''),0x1A, ''),0xefbfbd,''),0xefbebf,''),0xdc8e,''),0xdd8c,''),0xce81,''),0xce82,''),0xd68e,''),0x2ee1,''),0xe18ab1,'')" % name
        elif type.endswith('text'):
            query = "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(%s, '\\t', ' '), '\\n', ' '), '\\r', ' '), '\\0',''), 0x7F, ''),0x1A, ''),0xefbfbd,''),0xefbebf,''),0xdc8e,''),0xdd8c,''),0xce81,''),0xce82,''),0xd68e,''),0x2ee1,''),0xe18ab1,'')" % name
        elif type.find('char') != -1 and replaceType == REVIEW_REPLACEMENTS:
            query = "REPLACE(%s,'\\\\n', '\\\\\\\\n')" % name
        elif type.endswith('text') and replaceType == REVIEW_REPLACEMENTS:
            query = "REPLACE(%s,'\\\\n', '\\\\\\\\n')" % name

        if permitNull:
            query = "IFNULL(%s, '')" % query
        return query

    def _generateExtractionSQL(self, tablename, definitions, connectionInfo, extractInfo, fileInfo):
        """
        Generates extraction SQL.

        Args:
            definition: table definition
            connectionInfo: Information about the database we will connect to
            extractionInfo: Information about what/where to extract from
            fileInfo      : Information about how the data output will be formatted

        Returns:
            String sql
        """
        additionalColumns       = extractInfo['additional_columns']
        delimeter               = self._getDelimeter(fileInfo[DELIMETER])
        enclosedBy              = fileInfo[ENCLOSED_BY]
        columnNameSelectClause  = ""
        columnSelectClause      = ""

        # Column definition
        # [0]: Field
        # [1]: Type
        # [2]: Null (YES|NO)
        # [3]: Key
        # [4]: Default
        # [5]: Extra
        index = 1
        for definition in definitions:
            name        = definition['field'].encode('utf8')
            type        = definition['type'].encode('utf8')
            permitNull  = definition['nullable'] == u'YES'

            if index == 1 and enclosedBy != '':
                column                  = self._generateSelectClauseForColumn(name, type, permitNull, extractInfo[REPLACE_STYLE])
                column                  = "concat('%s',%s)" % (fileInfo[ENCLOSED_BY], column)
                columnNameSelectClause += "'%s', '" % name
            elif len(definitions) == index and enclosedBy != '' and additionalColumns == None:
                column                  = self._generateSelectClauseForColumn(name, type, permitNull, extractInfo[REPLACE_STYLE])
                column                  = "concat(%s,'%s')" % (column, fileInfo[ENCLOSED_BY])
                columnNameSelectClause += "'%s', " % name
            else:
                column                   = self._generateSelectClauseForColumn(name, type, permitNull, extractInfo[REPLACE_STYLE])
                columnNameSelectClause  += "'%s', " % name
            columnSelectClause += column + ", "
            index              += 1
        columnNameSelectClause = columnNameSelectClause[:-2]
        columnSelectClause     = columnSelectClause[:-2]

        # Add additional columns to each row
        if (additionalColumns):
           index = 1
           keyValPairs = additionalColumns.split(',')
           numColumns  = len(keyValPairs)
           for keyValPair in keyValPairs:
               (key, val) = keyValPair.split(':')
               val        = val.replace('<TABLENAME>',tablename)
               val        = val.replace('<HOST>', connectionInfo[HOST])
               val        = val.replace('<SCHEMA>', connectionInfo[SCHEMA])
               columnNameSelectClause += "','%s'" % key
               if index == numColumns and enclosedBy != '':
                  columnSelectClause      += ",'%s%s'" % (val, enclosedBy)
               else:
                  columnSelectClause      += ',' + "'" + val + "'"
               index += 1

        # Add Column Header Row If Requested
        sql         = ""
        if fileInfo[ADD_HEADER]:
            headerClause = "concat_ws('%s%s%s' , %s)" %  (enclosedBy, delimeter, enclosedBy, columnNameSelectClause)
            sql          = "SELECT %s \nUNION\n" % (headerClause)

        if extractInfo[MODE] == MODE_FULL:
            whereClause = ""
        elif extractInfo[TARGET_DATE_COLUMN]:
            whereClause = " WHERE DATE(%s) >= '%s' AND DATE(%s) <= '%s'" % (extractInfo[TARGET_DATE_COLUMN], extractInfo[START_DATE], extractInfo[TARGET_DATE_COLUMN], extractInfo[END_DATE])
        else:
            whereClause = ''

        if extractInfo[ADDITIONAL_CONDITIONS] and extractInfo[MODE] == MODE_FULL:
            whereClause += "%s" % (extractInfo[ADDITIONAL_CONDITIONS])

        # Build query for getting the data
        selectClause  = "SELECT concat_ws('%s%s%s',%s)" % (enclosedBy, delimeter, enclosedBy, columnSelectClause)
        sql          += "%s\nFROM %s\n%s" % (selectClause, tablename, whereClause)

        return sql

    def _getTableLevelConfiguration(self, tableName):
        '''
        Get information for the specified user from the mysql.cfg file
        '''
        tableLevelConfig = {}

        # Return the User this table
        user = self.config.get(self.options.get('table'), 'user')
        msg = "The user is set to '%s'" % (user)
        logging.debug(msg)
        tableLevelConfig['user'] = user

        schema = self.config.get(self.options.get('table'), 'schema')
        msg = "The schema is set to '%s'" % (schema)
        logging.debug(msg)
        tableLevelConfig['schema'] = schema

        loadDataFile = self.config.get(self.options.get('table') , 'load_data_file')
        msg = "Load Data File is set to '%s'" % (loadDataFile)
        logging.debug(msg)
        tableLevelConfig['load_data_file'] = loadDataFile

        return tableLevelConfig

    def _getSchemaLevelConfiguration(self, schema):
        '''
        Get schema level information for the specified schema from the mysql.cfg file
        '''
        sectionConfig = {}
        # Read the host and port for this schema
        schemaSection = "%s_schema" % (schema)
        sectionConfig['host'] = self.config.get(schemaSection         , 'host')
        sectionConfig['port'] = self.config.get(schemaSection         , 'port')
        return sectionConfig



    def _getUsernameAndPassword(self, user):
        '''
        Returns the username and password
        '''

        mysqlPasswordDirectory = self._readMandatoryOption(SECTION_DIRECTORIES, MYSQL_PASSWORD_DIRECTORY)
        passwordFile           = "%s/%s.cfg" % (mysqlPasswordDirectory, user)
        self.config.read(passwordFile)
        username               = self._readMandatoryOption(user, USERNAME)
        password               = self._readMandatoryOption(user, PASSWORD)

        return (username, password)

    #TODO: Remove or refactor this code to use _getUsernameAndPassword()
    #TODO: Remove this function as it is incorrect, as it should
    def _getUserLevelConfiguration(self, user):
        '''
        Get information for the specified user from the mysql.cfg file.
        '''
        sectionConfig = {}
        # Read the username/password for the specified user
        userSection = "%s_user" % (user)
        username = self.config.get(userSection,'username')
        sectionConfig['username'] = username

        password = self.config.get(userSection,'password')
        sectionConfig['password'] = password

        return sectionConfig


    def _logError(self, msg,stdout, stderr):
        logging.error(msg)
        if (len(stdout) > 0):
           logging.error('--STANDARD OUTPUT--')
           for line in stdout:
               logging.error(line.strip())

        if (len(stderr) > 0):
           logging.error('--STANDARD ERROR--')
           for line in stderr:
               logging.error(line.strip())

    def _readDatabaseSection(self , database):
        #Both of formats below can be dealed well.
        #database=sample_order
        #database=sample_order : In the future, this format will be gone
        database=database.replace('sample_','sample1_')

        databaseSection = "%s_%s" % (self.environment, database)
        dbInfo          = {}
        dbInfo[HOST]     = self._readMandatoryOption(databaseSection, HOST)
        dbInfo[PORT]     = self._readMandatoryOption(databaseSection, PORT)
        dbInfo[SCHEMA]   = self._readMandatoryOption(databaseSection, SCHEMA)

        user = self._readMandatoryOption(databaseSection, USER)
        (dbInfo[USERNAME], dbInfo[PASSWORD]) = self._getUsernameAndPassword(user)
        return dbInfo


    def _splitAndLoadData(self, inputFilename, username, password, host, port, schema):
        # Read the split commands location
        splitCommandLocation = self.config.get('DEFAULT','split_command')

        # Read the mysql_command location
        mysqlCommandLocation = self.config.get('DEFAULT','mysql_command')

        # Determine the current working directory
        origCwd = os.getcwd()
        msg = "The current working directory is %s" % (origCwd)
        logging.debug(origCwd)

        # Change to the work directory before doing the split
        workDir = "%s/work" % (origCwd)
        os.chdir(workDir)

        # Split the Input File
        splitCmd = "%s --suffix-length=10 --lines=%s %s %s" % (splitCommandLocation, self.options.lines_per_file, inputFilename, 'split_file_parts')
        logging.info(splitCmd)
        (stdin, stdout, stderr, returncode) = self._executeOsCommand(splitCmd)
        if (returncode != RC_NO_ERROR):
           msg = "Split command failed. Aborting Operation"
           self._logError(msg,stdout,stderr)
           return returncode

        # Read the load data template file
        loadDataTemplateFilePath = self.config.get(self.options.get('table'), 'load_data_template_file')
        msg = "'load_data_template_file' is '%s'" % (loadDataTemplateFilePath)

        workDirFiles = os.listdir(workDir)
        for workDirFile in workDirFiles:
            # Skip any file that is not created from the split
            if (workDirFile.find('split_file_parts') == -1):
               continue

            # Create the load data file from the template
            templateFile     = open(loadDataTemplateFilePath, 'r')
            templateContents = templateFile.read()
            templateContents = templateContents.replace('${INPUT_FILE}',workDirFile)

            loadFilename = "%s-load_data.sql" % (workDirFile)
            loadFile = open(loadFilename, 'w')
            loadFile.write(templateContents)

            loadFile.close()
            templateFile.close()

            msg = "Loading split file %s " % (workDirFile)
            logging.debug(msg)

            mysqlCmd = "%s -u%s -p%s -h%s -P%s %s < %s" % (mysqlCommandLocation, username, password, host, port, schema,loadFilename)
            (stdin, stdout, stderr, returncode) = self._executeOsCommand(mysqlCmd)
            if (returncode != 0):
               msg = "mysql load failed! Aborting operation"
               self._logError(msg,stdout,stderr)
               return returncode

            # Remove the split file and load file
            msg = "Removing split file %s" % (workDirFile)
            logging.debug(msg)
            os.unlink(workDirFile)

            msg = "Removing the load data file %s" % (loadFilename)
            logging.debug(msg)
            os.unlink(loadFilename)

        return RC_NO_ERROR

    def _validatePathExists(self, directoryName):
        # Check that the directory exists
        if (os.path.exists(directoryName) == False):
            msg = "'%s' does not exist! Aborting operation." % (directoryName)
            logging.error(msg)
            return RC_ERROR
        return RC_NO_ERROR

    def getTablePrimaryKey(self, database, tablename):
        tableDefinition = self.getTableDefinition(database, tablename)
        primaryKey = []
        for col in tableDefinition:
            if (col['primary_key'] == 'PRI'):
               primaryKey.append(col['field'])

        if len(primaryKey) == 0:
           primaryKey = None
        return primaryKey

    def getTableDefinition(self, database, tablename):
        descSql         = 'DESC %s' % tablename
        tableDefinition = []
        resultSet       = self.executeSql(database, descSql)
        for row in resultSet:
            rowInfo = {'field':row[0], 'type':row[1],'nullable':row[2],'primary_key':row[3],'default':row[4],'extra':row[5]}
            tableDefinition.append(rowInfo)
        return tableDefinition

    def executeSql(self, databaseKey, sql, parameters=None, isolation=0,fetch=100):
        """
        Execute a query and return the result set

        Args:
            databaseKey    :sample_order(e.g.)

            isolation
                0:'' (default isolation)
                1:'READ UNCOMMITTED'
                2:'READ COMMITTED'
                3:'REPEATABLE READ'
                4:'SERIALIZABLE'

            fetchmany : The number of record to be fetched as one time

        Note
            Python will recognize DB value up to JDBC Option(tinyInt1isBit,zeroDateTimeBehavior)
            Value(-128 - 127) for TINYINT(1) will be recognized by Python as follows

            Value(0000-00-00 00:00:00) for Timestamp will be recognized by Python as follows
                zeroDateTimeBehavior=exception(default): SQL error
                zeroDateTimeBehavior=convertToNull     : None
                zeroDateTimeBehavior=round             : 0001-01-01 00:00:00
        """
        #get MySQL Information
        mysql_info  = self._readDatabaseSection(databaseKey)
        schema      = mysql_info['schema']  # item etc.
        host        = mysql_info['host']
        port        = mysql_info['port']
        username    = mysql_info['username']
        password    = mysql_info['password']

        other      = '?tinyInt1isBit=false&zeroDateTimeBehavior=convertToNull'

        url = 'jdbc:mysql://%s:%s/%s%s' % (host, port, schema,other)
        logging.debug('JDBC URL=%s' % url)
        # Parameters not included
        logging.debug('SQL=%s' % sql)

        jdbc = None
        try:
            if isolation==0:
                isolation=''
            if isolation==1:
                isolation='READ UNCOMMITTED'
            if isolation==2:
                isolation='READ COMMITTED'
            if isolation==3:
                isolation='REPEATABLE READ'
            if isolation==4:
                isolation='SERIALIZABLE'

            jdbc = JDBC(url, 'com.mysql.jdbc.Driver', username, password,isolation)

            # Execute SQL
            cursor = jdbc.execute(sql, parameters)

            result_list = []

            #fetchmany start
            records = cursor.fetchmany(fetch)
            while records:
                for record in records:
                    result_list.append(record)
                records = cursor.fetchmany(fetch)
            #fetchmany end

        except Exception, msg:
            logging.error(msg)
            raise

        return result_list

    def executeDDL(self,  databaseKey, sql,isolation=0):
        """
        Executes a SQL(INSERT , DELETE etc.) through JDBC.

        Args:
            databaseKey :sample_order(e.g.)
            sql         :SQL

            isolation
                0:'' (default isolation)
                1:'READ UNCOMMITTED'
                2:'READ COMMITTED'
                3:'REPEATABLE READ'
                4:'SERIALIZABLE'

        Returns:
            result code

        Raises:
            Exception: databaseKey connection error
        """
        #get MySQL Information
        mysql_info  = self._readDatabaseSection(databaseKey)
        schema      = mysql_info['schema']  # item etc.
        host        = mysql_info['host']
        port        = mysql_info['port']
        username    = mysql_info['username']
        password    = mysql_info['password']

        url = 'jdbc:mysql://%s:%s/%s' % (host, port, schema)

        jdbc = None
        try:
            if isolation==0:
                isolation=''
            if isolation==1:
                isolation='READ UNCOMMITTED'
            if isolation==2:
                isolation='READ COMMITTED'
            if isolation==3:
                isolation='REPEATABLE READ'
            if isolation==4:
                isolation='SERIALIZABLE'

            jdbc = JDBC(url, 'com.mysql.jdbc.Driver', username, password,isolation)

            # Execute SQL
            result=jdbc.execute(sql)

        except Exception, msg:
            logging.error(msg)
            raise
        return result

    def hasTable(self, databaseKey, table_name, parameters=None):
        """
        Check table existence through JDBC.

        Args:
            databaseKey   : sample_order(e.g.)
            table_name    : table name
            parameters    : SQL statement parameters

        Returns:
            hasTable : True or False
        """
        #get MySQL Information
        mysql_info  = self._readDatabaseSection(databaseKey)
        schema      = mysql_info['schema']  # item etc.
        host        = mysql_info['host']
        port        = mysql_info['port']
        username    = mysql_info['username']
        password    = mysql_info['password']

        hasTable    = True
        url = 'jdbc:mysql://%s:%s/%s' % (host, port, schema)
        logging.debug('JDBC URL=%s' % url)
        # Parameters not included

        sql  = "SHOW TABLES FROM `%s` LIKE '%s'" %  (schema,table_name)
        logging.debug('SQL=%s' % sql)

        jdbc = JDBC(url, 'com.mysql.jdbc.Driver', username, password)

        # Execute SQL
        cursor = jdbc.execute(sql, parameters)

        record = cursor.fetchone()
        if record is None:
            hasTable   = False

        return hasTable

    def getTableList(self, databaseKey, tablePattern = None):
        """
        Get table list

        Args:
            databaseKey    : sample_order(e.g.)

        Returns:
             tableList
        """
        tableList=[]

        tablenameClause = ""
        if (tablePattern):
           tablenameClause = "AND table_name like '%s'\n" % (tablePattern)

        mysql_info  = self._readDatabaseSection(databaseKey)
        schema      = mysql_info['schema']  # item etc.
        sql         = """
                      SELECT table_name
                      FROM   information_schema.tables
                      WHERE  table_schema = '%s'
                             %s
                      """ % (schema, tablenameClause)

        recordSet = self.executeSql(databaseKey, sql)
        for rec in recordSet:
            tablename      = rec[0].encode('utf8').strip()
            tableList.append(tablename)

        return tableList

    def mysqlLoadData(self):
        '''
        Loads data into the specified table by calling mysql and passing in a load file
        '''
        self._validateOptions(['table'],{})
        msg = "Entering MYSQL.mysqlLoadData function"
        logging.debug(msg)   # TODO: use decorators to handle pre/post function call level logging. (w/ introspection)
        logging.info('Loading Data')


        # Validate that the requested section in the config file exists
        if (self.config.has_section(self.options.get('table')) == False):
           msg = "No configuration found for '%s'. Aborting process." % (self.options.get('table'))
           logging.error(msg)
           return RC_ERROR


        # Return the User,Schema, and Load Data File for this table
        user = self.config.get(self.options.get('table'), 'user')
        msg = "The user is set to '%s'" % (user)
        logging.debug(msg)

        schema = self.config.get(self.options.get('table'), 'schema')
        msg = "The schema is set to '%s'" % (schema)
        logging.debug(msg)

        loadDataFile = self.config.get(self.options.get('table') , 'load_data_file')
        msg = "Load Data File is set to '%s'" % (loadDataFile)
        logging.debug(msg)

        # Read the username/password for the specified user
        userSection = "%s_user" % (user)
        username = self.config.get(userSection,'username')
        password = self.config.get(userSection,'password')


        # Read the host and port for this schema
        schemaSection = "%s_schema" % (schema)
        host = self.config.get(schemaSection         , 'host')
        port = self.config.get(schemaSection         , 'port')

        # Read the mysql_command location
        mysqlCommandLocation = self.config.get('DEFAULT','mysql_command')

        mysqlCmd = "%s -u%s -p%s -h%s -P%s %s < %s" % (mysqlCommandLocation, username, password, host, port, schema,loadDataFile)
        self._executeOsCommand(mysqlCmd)

        return RC_NO_ERROR

    def mysqlSplitThenLoadData(self):
        '''
        Splits the input file, then loads the specified table by calling mysql and passing in the parsed load file.
        TODO: Consider merging with mysqlLoadData
        '''
        self._validateOptions(['table','lines_per_file'],{})

        # Validate that the requested section in the config file exists
        if (self.config.has_section(self.options.get('table')) == False):
           msg = "No configuration found for '%s'. Aborting process." % (self.options.get('table'))
           logging.error(msg)
           return RC_ERROR

        # Read the user
        user = self.config.get(self.options.get('table'), 'user')
        msg = "The user is set to '%s'" % (user)
        logging.debug(msg)

        # Read the schema
        schema = self.config.get(self.options.get('table'), 'schema')
        msg    = "The schema is set to '%s'" % (schema)
        logging.debug(msg)

        # Read the username/password for the specified user
        userSection = "%s_user" % (user)
        username = self.config.get(userSection,'username')
        password = self.config.get(userSection,'password')

        # Read the host and port for this schema
        schemaSection = "%s_schema" % (schema)
        host = self.config.get(schemaSection         , 'host')
        port = self.config.get(schemaSection         , 'port')

        # Determine the Input Files that should be loaded
        msg = "Determining Input Files"
        logging.info(msg)
        inputFiles = []
        if (self.options.input_file_directory != None and self.options.input_file_pattern == None):
            # Add a debug log message
            msg = "'input_file_directory' has been set to '%s'. All files in this directory will be considered input files" % (self.options.input_file_directory)
            logging.debug(msg)

            # Check that the directory exists
            returncode = self._validatePathExists(self.options.input_file_directory)
            if (returncode != RC_NO_ERROR):
               return returncode

            # Add all files to the inputFiles array
            for filename in os.listdir(self.options.input_file_directory):
                fullFilename = "%s/%s" % (self.options.input_file_directory, filename)
                msg = "'%s' will be used as an input file" % (fullFilename)
                logging.debug(msg)
                inputFiles.append(fullFilename)
        elif (self.options.input_file_directory != None and self.options.input_file_pattern != None):
            # Add a debug Log Message
            msg = "'input_file_directory' is '%s'. 'input_file_pattern' is '%s'" % (self.options.input_file_directory,self.options.input_file_pattern)
            logging.debug(msg)

            # Check that the directory exists
            returncode = self._validatePathExists(self.options.input_file_directory)
            if (returncode != RC_NO_ERROR):
               return returncode

            # Add all files that match input_file_pattern
            for filename in os.listdir(self.options.input_file_directory):
                fullFilename = "%s/%s" % (self.options.input_file_directory, filename)
                if (fullFilename.find(self.options.input_file_pattern) != -1):
                    msg = "'%s' will be used as an input file" % (fullFilename)
                    logging.debug(msg)
                    inputFiles.append(fullFilename)
        elif (self.options.input_file):
            # Add a debug log message
            msg = "'input_file' is '%s' " % (self.options.input_file)
            logging.debug(msg)

            # Check that the file exists
            returncode = self._validatePathExists(self.options.input_file_directory)
            if (returncode != RC_NO_ERROR):
               return returncode

            inputFiles.append(self.options.input_file)
        else:
            # Add a debug log message
            msg               = "No input file information was supplied. Checking config for 'input_file_location'"
            logging.debug(msg)

            # Read 'input_file_location' from the config file
            inputFileLocation = self.config.get(self.options.get('table'), 'input_file_location')

            # Check that the file exists
            returncode = self._validatePathExists(inputFileLocation)
            if (returncode != RC_NO_ERROR):
               return returncode

            inputFiles.append(inputFileLocation)

        # Validate that we have indeed found input files to load
        if (len(inputFiles) == 0):
           msg = "No input files found! Process aborting."
           logging.error(msg)
           return RC_ERROR

        # For each file found, load it into mysql
        for inputFile in inputFiles:
            msg = "Now loading the file '%s'" % (inputFile)
            logging.info(msg)
            returncode = self._splitAndLoadData(inputFile, username, password, host, port, schema)
            if (returncode != RC_NO_ERROR):
               msg = "Loading Failed! Aborting Process."
               logging.error(msg)
               return returncode
            msg = "Loading Finished"
            logging.info(msg)

        return RC_NO_ERROR

    def mysqlTruncateTable(self):
        '''
        Truncates the contents of the specified table

        --TODO--
        1. Add a -no_warn flag, show warnings if flag is not supplied
        '''
        # Validate that we have the correct command line arguments for this function
        self._validateOptions(['table'],{})

        # Get all necessary configuration information
        tableConfig  = self._getTableLevelConfiguration(self.options.get('table'))
        userConfig   = self._getUserLevelConfiguration(tableConfig['user'])
        schemaConfig = self._getSchemaLevelConfiguration(tableConfig['schema'])

        # Create an .sql file and write the truncate statement to the file
        truncateFilename = 'mysql_truncate.sql'
        tempfile = open(truncateFilename,'w')
        sql = "TRUNCATE %s;\n" % (self.options.get('table'))
        msg = "SQL in file is [%s]" % (sql)
        logging.debug(msg)
        tempfile.write(sql)

        # Read the mysql_command location
        mysqlCommandLocation = self.config.get('DEFAULT','mysql_command')

        # Execute mysql passing in the newly created file
        mysqlCmd = "%s -u%s -p%s -h%s -P%s %s < %s" % (mysqlCommandLocation, userConfig['username'], userConfig['password'], schemaConfig['host'],
                                                       schemaConfig['port'], tableConfig['schema'], truncateFilename)
        (stdin, stdout, stderr, returncode) = self._executeOsCommand(mysqlCmd)

        # Remove the file we just created
        os.remove(truncateFilename)

        return returncode

    def createExtractSql(self, databaseKey, scheme, table, columnStr, delimiter, dateColumn=None, startDate=None, endDate=None):
        columnTypes = self.getTableColumnTypes(databaseKey, scheme, table)
        if not columnStr:
            logging.info("All columns will be extracted.")
            columns = self.getTableColumns(databaseKey, scheme, table)
        else:
            columns = [column.strip() for column in columnStr.split(',')]
        columnExps = []
        for column in columns:
            columnName = column.strip('\`')
            if 'date' in columnTypes[columnName].lower():
                columnExps.append("IFNULL(CASE WHEN %s < '0000-01-01' THEN NULL ELSE %s END,'')" % (column, column))
            elif 'binary(16)' in columnTypes[columnName].lower():
                columnExps.append("LOWER(CONCAT_WS('-', SUBSTRING(HEX(%s),1,8), SUBSTRING(HEX(%s),9,4), SUBSTRING(HEX(%s),13,4), SUBSTRING(HEX(%s),17,4), SUBSTRING(HEX(%s),21,12)))" % (column, column, column, column, column))
            elif 'char' in columnTypes[columnName].lower() or 'text' in columnTypes[columnName].lower():
                columnExps.append("REPLACE(REPLACE(REPLACE(REPLACE(IFNULL(%s,''),'\\0',''),'\\t',' '),'\\n',' '),'\\r',' ')" % (column))
            else:
                columnExps.append("IFNULL(%s,'')" % (column))
        columnPart = ','.join(columnExps)
        if (not dateColumn):
            logging.info("Extract in FULL mode, all records will be extracted.")
            whereClause = ""
        else:
            whereClause = "WHERE DATE(%s) >= '%s' AND DATE(%s) <= '%s'" % (dateColumn, startDate, dateColumn, endDate)
        if delimiter.decode('string_escape') == '\t':
            sql = "SELECT %s FROM %s %s" % (columnPart, table, whereClause)
        else:
            sql = "SELECT CONCAT_WS('%s',%s) FROM %s %s" % (delimiter, columnPart, table, whereClause)
        return sql

    def createExtractCmd(self, database, table, columns, delimiter, outputFile, dateColumn=None, startDate=None, endDate=None, characterSet='utf8'):
        dbInfo = self._readDatabaseSection(database)

        # Generate SQL
        sql = self.createExtractSql(database, dbInfo[SCHEMA], table, columns, delimiter, dateColumn, startDate, endDate)

        # Generate Execution Command
        mysql = self.config.get(SECTION_OS_COMMANDS, MYSQL_COMMAND)
        cmd = "%s --default-character-set=%s -h%s -P%s -u%s -p%s %s --skip-column-names -r -e \"%s\" > %s" % (mysql, characterSet, dbInfo[HOST], dbInfo[PORT], dbInfo[USERNAME], dbInfo[PASSWORD], dbInfo[SCHEMA], sql, outputFile)
        return cmd

    def extractToFile(self, parameters):
        # Create extract command
        parameters[OUTPUT_FILE] = os.path.join(parameters[TARGET_DIRECTORY], parameters[OUTPUT_FILENAME])
        parameters[WORK_FILE] = parameters[OUTPUT_FILE] + '_' + str(uuid.uuid4())
        cmd = self.createExtractCmd(parameters[DATABASE], parameters[TABLE], parameters[COLUMNS], parameters[DELIMITER], parameters[WORK_FILE], parameters[TARGET_DATE_COLUMN], parameters[START_DATE], parameters[END_DATE], parameters[CHARACTER_SET])

        # Execute extract command
        logging.info("Now extracting by mysql command...")
        (stdin, stdout, stderr, returncode) = self._executeOsCommand(cmd)
        logging.info("mysql execution finished.")

        # Output command log
        if (returncode != 0):
            msg = "Extract failed with return code %d." % returncode
            logLevel = 'error'
        else:
            msg = "Extract completed successfully."
            logLevel = 'debug'
            # Rename to target output file
            shutil.move(parameters[WORK_FILE], parameters[OUTPUT_FILE])
            logging.info("The extract file %s was created." % (parameters[OUTPUT_FILE]))
        self._log(msg,stdout,stderr,logLevel)
        return returncode
