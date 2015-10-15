# -*- coding: utf-8 -*-
import codecs
import logging
import os.path
import sys
import uuid
from datetime import datetime, timedelta

from modules.base.jdbc        import JDBC
from modules.base.constants   import *
from modules.base.exceptions  import ReleaseTableMissing
from modules.base.database    import Database
from modules.base.environment import Environment
import modules.base.etl_util as etl_util

class TeradataDB(Database):
    '''
    This class is for all Teradata DB specific functionality
    '''
    def __init__(self):
        super(TeradataDB, self).__init__()
        self.config.read(os.path.join(etl_util.get_app_root_path(), 'conf/modules/teradata.cfg'))

        self.FAST_EXPORT_BTQ_TEMPLATE_FILENAME       = 'templates/teradata/fast_export/etl_api.btq.tmpl'

    #####################
    # Private Functions #
    #####################
    def _isLobTable(self, helpColumns):
        for columnInfo in helpColumns:
            if columnInfo[TYPE].strip().encode('utf8') in TERADATA_LOB_TYPES:
                return True
        return False

    #TODO: Move the to fastexport class
    def _createFastExportControlFile(self, executionId, origSql, seperatedBy, enclosedBy, newlineText, rolename, outfileLocation, fastExportMaxLineLength, resultSetTop):
        '''
        Creates a fast export control file from the given SQL
        '''
        logging.debug("start of Teradata._createFastExportControlFile()")

        # Convert the sql to lowercase
        lowerSql = str(origSql).lower()

        # Strip any comments from the sql
        lowerSql = self._stripComments(lowerSql)

        # Remove all duplicated white space
        lowerSql = " ".join(lowerSql.split())

        parts = lowerSql.split('from')
        selectClause = parts[0]
        restOfQuery  = ' from ' + parts[1]

        # Get the list of Fields
        fields  = self._getColumnFields(lowerSql, rolename)

        # Determine the seperator to use
        if (seperatedBy == 'comma'):
             seperator  = ","
        elif (seperatedBy == 'tab'):
             seperator  = "\t"
        elif (seperatedBy == 'pipe'):
             seperator  = '|'

        # Determine newline
        if (newlineText == 'windows'):
            newline = " || '\r\n'"
        elif (newlineText == 'unix'):
            newline = " || '\n'"
        elif (newlineText    == 'nothing'):
            newline = ''

        if (enclosedBy == 'single-quote'):
            #startQuotation = " '\\\'' ||"
            #endQuotation   = "|| '\\\'' "
            startQuotation = " '''' ||"
            endQuotation   = "|| '''' "
        elif (enclosedBy == 'double-quote'):
            startQuotation = ' \'"\' ||'
            endQuotation   = '|| \'"\''
        elif (enclosedBy == 'nothing'):
            startQuotation = ''
            endQuotation   = ''

        # Load the date/time/timestamp column lists
        columnsString = self.config.get(SECTION_TERADATA, 'date_columns')
        dateColumns = columnsString.split(',')

        columnsString = self.config.get(SECTION_TERADATA, 'time_columns')
        timeColumns = columnsString.split(',')

        columnsString = self.config.get(SECTION_TERADATA, 'timestamp_columns')
        timestampColumns = columnsString.split(',')

        # Add the top clause if needed
        if (resultSetTop == None):
           topClause = ""
        else:
           topClause = " TOP %s " % (resultSetTop)

        # Build the new select statement
        selectClause = "select %s cast(" % (topClause)
        commaReplacement = ""
        for field in fields:
            columnName = field.split('.')[-1].strip()
            if (columnName in dateColumns):
                selectClause += ' ' + commaReplacement + startQuotation + "trim(coalesce(cast(" +  field.strip() + " as char(19)),''))" + endQuotation
            elif (columnName in timeColumns):
                selectClause += ' ' + commaReplacement + startQuotation + "trim(coalesce(cast(" +  field.strip() + " as char(19)),''))" + endQuotation
            elif (columnName in timestampColumns):
                selectClause += ' ' + commaReplacement + startQuotation + "trim(coalesce(cast(" +  field.strip() + " as char(19)),''))" + endQuotation
            else:
                selectClause += ' ' + commaReplacement + startQuotation + "trim(coalesce(" +  field.strip() + ",''))" + endQuotation
            commaReplacement = " || '%s' || " % (seperator)
        selectClause += newline + " as char(%s)) " % (fastExportMaxLineLength)

        fastExportSQL = selectClause + restOfQuery

        # Read the Template File
        templateFile = open(self.FAST_EXPORT_BTQ_TEMPLATE_FILENAME,'r')
        btqContents =  templateFile.read()
        templateFile.close()

        # Make any Substitutions needed
        btqContents =  btqContents.replace('<QUERY>', fastExportSQL)

        # Substitute the correct logon string
        (funcReturnCode, teraPassData) = self._parseTeraPass(rolename)
        logonString                    = "red/%s,%s" % (rolename, teraPassData['PASSWORD'])
        btqContents                    = btqContents.replace('<LOGON_STRING>', str(logonString))

        # Substitute the outfile location
        btqContents = btqContents.replace('<OUTFILE_LOCATION>',outfileLocation)

        # Substitute the logtable name
        logtableName = "etl_api_error_%s" % (executionId)
        btqContents = btqContents.replace('<LOGTABLE_NAME>',logtableName)

        fastExportBtqFilename = "work/fast-export-query-id-%s" % (executionId)
        fastExportBtq = open(fastExportBtqFilename, 'w')
        fastExportBtq.write(btqContents)

        return fastExportBtqFilename

    def executeBteq(self, filename, variables, database):
        '''
        Substitutes variables into bteq file using template
        and executes bteq file
        '''
        # Read template
        allLines = open(filename).read()
        # Determine ML_JOBNET_NAME
        jobnet_name = 'DEV_SAMPLE'
        if os.environ.has_key('ML_JOBNET_NAME'):
             jobnet_name = os.environ['ML_JOBNET_NAME']

        # Parse terapass info and substitutes
        logging.info("database name is '%s'" % database)
        teraPassVariables = self._parseTeraPass(database)[1]

        for key, val in teraPassVariables.iteritems():
            if key == 'LOGON_USER' :
                replaceQUERYBAND= val.replace("\n", "").replace("\"", "") + ";\n" + "SET QUERY_BAND = 'JOBNETNAME=" + jobnet_name + ";JOBNAME=" + os.path.basename(filename) + ";' FOR SESSION"
                logging.debug("logon add following information '%s'", replaceQUERYBAND)
                allLines = allLines.replace('<%s>' % key, replaceQUERYBAND)
            else:
                allLines = allLines.replace('<%s>' % key, val.replace("\n", "").replace("\"", ""))

        # Substitute valiables from option
        for key,val in variables.items():
            logging.debug('variable %s=%s', key, val)
            try:
                if key in teraPassVariables:
                    logging.warn('%s in --variables is not applicable and is skipped', key)
                allLines = allLines.replace('<%s>' % key, val)
            except ValueError:
                logging.error("Wrong Arguments for --variables")
                return RC_ERROR

        # Get Work Directory
        workDir = self.config.get('BTEQ', 'WORK_DIR')
        logging.debug("Work Directory Set to '%s'" % (workDir))

        # Write bteq file
        baseName     = filename[filename.rfind('/') + 1:filename.rfind('.btq')]
        bteqFilename = '%s_%s_%s.btq' % (baseName, self._createDateString('YYYYMMDDhhmiss'), str(uuid.uuid4()))
        bteqFile     = os.path.abspath(os.path.join(workDir, bteqFilename))
        logging.info('bteq file : ' + bteqFile)

        # The workDir definition can have (and often does) a relative path.
        # Assuring the whole path exists, and not only the workDir part.
        try:
            self._createWorkDirectory(os.path.split(bteqFile)[0])
        except:
            return RC_ERROR_COULD_NOT_CREATE_DIRECTORY

        f = open(bteqFile, 'w')
        f.write(allLines)
        f.close()

        # Execute bteq command
        bteq = self.config.get(SECTION_TERADATA, 'BTEQ_COMMAND')
        bteqCmd = "%s < %s" % (bteq, bteqFile)
        (stdin, stdout, stderr, returncode) = self._executeOsCommand(bteqCmd)

        # Print log
        if (returncode == 0):
            msg = "bteq completed"
            logLevel = 'debug'
            logging.debug(msg + ' (check details for log file)')
        else:
            msg = "bteq failed with return code %d" % returncode
            logLevel = 'info'
        self._log(msg,stdout,stderr,logLevel)

        # Remove bteq file
        os.remove(bteqFile)

        # Return with returncode, RC_NO_ERROR or RC_ERROR
        return returncode

    def executeBteqShell(self, filename, variables, database):
        '''
        Substitutes variables into bteq shell file using template
        and executes bteq shell file
        '''
        # Read template
        allLines = open(filename).read()

        # Parse terapass info and substitutes
        logging.info("database name is '%s'" % database)
        teraPassVariables = self._parseTeraPass(database)[1]
        # Determine ML_JOBNET_NAME
        jobnet_name = 'DEV_SAMPLE'
        if os.environ.has_key('ML_JOBNET_NAME'):
             jobnet_name = os.environ['ML_JOBNET_NAME']

        for key, val in teraPassVariables.iteritems():
            if key == 'LOGON_USER' :
                replaceQUERYBAND= val.replace("\n", "").replace("\"", "") + ";\n" + "SET QUERY_BAND = 'JOBNETNAME=" + jobnet_name + ";JOBNAME=" + os.path.basename(filename) + ";' FOR SESSION"
                logging.debug("logon add following information '%s'", replaceQUERYBAND)
                allLines = allLines.replace('<%s>' % key, replaceQUERYBAND)
            else:
                allLines = allLines.replace('<%s>' % key, val.replace("\n", "").replace("\"", ""))

        # Substitute valiables from option
        for key,val in variables.items():
            logging.debug('variable %s=%s', key, val)
            try:
                if key in teraPassVariables:
                    logging.warn('%s in --variables is not applicable and is skipped', key)
                if key=='second_process_id':
                    workDir = self.config.get('BTEQ', 'WORK_DIR')
                    baseName     = filename[filename.rfind('/') + 1:filename.rfind('.btq')]
                    val='%s/%s_%s_%s.btq' % (workDir,baseName,self._createDateString('YYYYMMDDhhmiss'),val)
                allLines = allLines.replace('<%s>' % key, val)
            except ValueError:
                logging.error("Wrong Arguments for --variables")
                return RC_ERROR

        # Get Work Directory
        workDir = self.config.get('BTEQ', 'WORK_DIR')
        logging.debug("Work Directory Set to '%s'" % (workDir))

        # Write bteq shell file
        baseName     = filename[filename.rfind('/') + 1:filename.rfind('.btq')]
        bteqShellFilename = '%s_%s_%s.btq' % (baseName, self._createDateString('YYYYMMDDhhmiss'), str(uuid.uuid4()))
        bteqShellFile     = os.path.abspath(os.path.join(workDir, bteqShellFilename))
        logging.info('bteq shell file : ' + bteqShellFile)

        # The workDir definition can have (and often does) a relative path.
        # Assuring the whole path exists, and not only the workDir part.
        try:
            self._createWorkDirectory(os.path.split(bteqShellFile)[0])
        except:
            return RC_ERROR_COULD_NOT_CREATE_DIRECTORY

        f = open(bteqShellFile, 'w')
        f.write(allLines)
        f.close()

        # Execute bteq shell command
        bteqSh = self.config.get(SECTION_TERADATA, 'SHELL_COMMAND')
        bteqShCmd = "%s < %s" % (bteqSh, bteqShellFile)
        (stdin, stdout, stderr, returncode) = self._executeOsCommand(bteqShCmd)

        # Print log
        if (returncode == 0):
            msg = "bteq shell completed"
            logLevel = 'debug'
            logging.debug(msg + ' (check details for log file)')
        else:
            msg = "bteq shell failed with return code %d" % returncode
            logLevel = 'info'
        self._log(msg,stdout,stderr,logLevel)

        # Remove bteq shell file
        os.remove(bteqShellFile)

        # Return with returncode, RC_NO_ERROR or RC_ERROR
        return returncode

    def _executeFastExport(self, filename, maxSessions, characterSet):
        '''
        Execute the 'fexp' teradata utility
        '''
        logging.debug("start of Teradata._executeFastExport()")
        logging.debug("Filename: %s" % filename)
        logging.debug("Max Sessions: %s" % maxSessions)

        # Variable Instantiation
        errorMsg = None

        # Get the location of the fexp command
        fexpCmdPath = self.config.get(SECTION_RED_TERADATA,'fexp_command')

        # Execute Fast Export
        fexpCmd = "%s -M %s -c %s < %s" % (fexpCmdPath, maxSessions, characterSet, filename)
        (stdin, stdout, stderr, returncode) = self._executeOsCommand(fexpCmd)
        logging.info('fexpCmd: %s' % fexpCmd)

        # Log anything from stderr
        for line in stderr:
            logging.error(line)

        # Log anything from stdout
        for line in stdout:
            if (line.find('RDBMS failure, 3523') > -1):
               errorMsg = "The user does not have SELECT access"
            elif (line.find('RDBMS failure, 3707') > -1):
               errorMsg = "Syntax error detected in the statement"
            elif (line.find('RDBMS failure, 2507: Out of spool space on disk.') > -1):
               errorMsg = "Out of spool space on disk"
            logging.info(line)

        # Return a generic error message if we have not returned a more specific error message
        if (errorMsg == None and returncode == self.FEXP_RETURN_CODE_WARNING):
           errorMsg = "Warning"
        elif (errorMsg == None and returncode == self.FEXP_RETURN_CODE_USER_ERROR):
           errorMsg = "User error"
        elif (errorMsg == None and returncode == self.FEXP_RETURN_CODE_FATAL_ERROR):
           errorMsg = "Fatal error"
        elif (errorMsg == None and returncode == self.FEXP_RETURN_CODE_NO_MESSAGE_DESTINATION):
           errorMsg = "No Message Destination"

        return (returncode, errorMsg)

    def _formatFastExportOutputFile(self, inputFilename, outputFilename, encoding, newlineText, header=None):
        '''
        Strips the excess trailing characters.
        Appends the header row to the file
        Counts the number of lines in the file
        '''
        # Debug logging
        logging.debug("start of Teradata._formatFastExportOutputFile()")
        logging.debug("Input Filename: %s"  % (inputFilename))
        logging.debug("Output Filename: %s" % (outputFilename))
        logging.debug("Encoding: %s"        % (encoding))
        logging.debug("Newline Text: %s"    % (newlineText))
        logging.debug("Header: %s"          % (header))


        # Determine newline
        if (newlineText == 'windows'):
            newline = "\r\n"
        elif (newlineText == 'unix'):
            newline = "\n"
        elif (newlineText    == 'nothing'):
            newline = ''

        inputFile  = open(inputFilename, 'r')
        outFile    = open(outputFilename, 'w')

        # Append the header as needed
        count = 0
        if (header):
           outFile.write(header)

        for line in inputFile:
            newString  = line.strip()
            newString += newline

            unicodeString = newString.decode('utf_8')
            encodedString = unicodeString.encode(encoding)

            outFile.write(encodedString)
            count     += 1

        outFile.close()
        inputFile.close()
        return (count, RC_NO_ERROR)

    def _getJDBC(self, user):
        '''
        Instanciate JDBC class
        '''
        (rc, variables) = self._parseTeraPass(user)
        user     = variables['USER_LOGON'].replace("\n", "").replace("\"", "")
        password = variables['PASSWORD'].replace("\n", "").replace("\"", "")

        envJdbcSection  = "%s_%s" % (self.environment, SECTION_JDBC)

        username = variables['USER_LOGON'].replace("\n", "").replace("\"", "")
        connect  = self.config.get(envJdbcSection, CONNECT)
        driver   = self.config.get(envJdbcSection, DRIVER)
        logging.info('user : %s' % user)

        return JDBC(connect, driver, user, password)

    def _closeAllJDBC(self):
        '''
        Close all JDBC connections (only for current thread)
        '''
        JDBC()

    def _getColumnNames(self, databaseName, tableName, rolename):
        '''
        Returns all column names of table
        '''

        # create sql to get column name
        sql = 'SELECT ColumnName FROM DBC.ColumnsV WHERE DatabaseName=\'%s\' AND TableName=\'%s\';' % (databaseName,tableName)

        # connect to Teradata by JDBC
        conn = self._getJDBC(rolename)

        # execute sql
        curs = conn.execute(sql)
        results =  curs.fetchall()

        # deal with results
        if results == None:
            logging.error("Error! empty result.")
            sys.exit(1)
        cleanFields = []
        for result in results:
            cleanFields.append(result[0].strip().encode('utf8'))
        curs.close()
        conn._close()

        return cleanFields

    def _getColumnFields(self, origSql, rolename):
        '''
        Returns column fields found in the select statement
        '''
        columnFields  = self._parseSelectStatement(origSql)

        # return fields if there is no star
        if columnFields[0].count('*') == 0:
            return columnFields

        # translate star to all column names
        tableFields = self._parseFromStatement(origSql)
        cleanFields = []
        for tableField in tableFields:
            databaseName = tableField.split('.')[0]
            tableName = tableField.split('.')[1]
            columnNames = self._getColumnNames(databaseName, tableName, rolename)
            for columnName in columnNames:
                cleanFields.append(tableField + '.' + columnName)

        return cleanFields

    def _parseTeraPass(self, user):
        '''
        Parse the environment variables in the teradata password directory
        '''
        variables      = {}
        teradataPasswordDirectory = self.config.get(SECTION_DIRECTORIES , PASSWORD_TERADATA_DIRECTORY)
        tdpId                     = self.config.get(SECTION_TERADATA , TDPID)


        # Open the password file
        filename     = ".tera_%s_pass" % (str(user).lower())
        fullFilePath = "%s/%s" % (teradataPasswordDirectory, filename)

        # Fix for Windows development
        if os.name == 'nt':
            # Scratching my functional programming itch
            cygPath = ['C:\\', 'cygwin64'] + filter(lambda a: a != '', fullFilePath.split('/'))
            fullFilePath = os.path.join(*cygPath)

        #logging.debug("tera pass file is: %s" % fullFilePath)
        passwordFile = open(fullFilePath, 'r')
        for line in passwordFile:
            if (line.find(TERA_PASS_USER_LOGON) == 0):
               line                                            = line.replace('${ENV_ID}', self.environmentId)
               fields                                          = line.split('=')
               userLogonVal                                    = fields[1]
               variables[TERA_PASS_USER_LOGON] = userLogonVal
            elif (line.find(TERA_PASS_PASSWORD) == 0):
               passwordVal = line.split('=')[1]
               variables[TERA_PASS_PASSWORD] = passwordVal
            elif (line.find(TERA_PASS_LOGON_USER) == 0):
               line         = line.replace('${TDPID}', tdpId)
               line         = line.replace('${USER_LOGON}', variables[TERA_PASS_USER_LOGON])
               line         = line.replace('${PASSWORD}'  , variables[TERA_PASS_PASSWORD])
               logonUserVal = line.split('=')[1]
               variables[TERA_PASS_LOGON_USER] = logonUserVal
        return (RC_NO_ERROR, variables)

    ####################
    # Public Functions #
    ####################
    def checkAmpBias(self, logicalDB, acutalDB):
        '''
        Check the AMP biases for tables in a given database
        '''
        select = """
                 SELECT trim(databasename)
                        , trim(tablename)
                        , (stddev_pop(currentperm)/average(currentperm)) as AmpBias
                        , (min(currentperm) / max(currentperm))          as MinMaxRatio
                        , sum(currentperm) /1024 / 1024                  as TableSize
                        , max(currentperm)                               as MaxAmpSize
                 FROM dbc.tablesize
                 WHERE databasename not like 'DEV_%'
                       AND  databasename not like 'dbc%'
                       AND  databasename not like 'SYS%'
                       AND  databasename not like 'tdwm%'
                       AND  databasename not like 'SQLJ%'
                       AND  tablename    not like 'PMERR%'
                   GROUP BY tablename,databasename
                   ORDER BY 1,3 desc
                """
        resultSet = self.executeSql(logicalDB, select, convertToHash=True)
        return resultSet

    def checkDatabaseSize(self, logicalDB, actualDB):
        '''
        Check the size of a given database
        '''
        select = """
                 select tablename , sum(currentperm)
                 from dbc.tablesize
                 where databasename = '%s'
                 group by 1
                 order by 1
                 """ % (actualDB)
        logging.info(select)
        resultSet = self.executeSql(logicalDB, select)

        # Format the results
        dataSet       = {}
        tables        = []
        dbCurrentPerm = 0
        for row in resultSet:
            dbCurrentPerm += int(row[1])
            tableData      = {'tablename':row[0], 'current_perm':row[1] }
            tables.append(tableData)
        dataSet['tables']       = tables
        dataSet['current_perm'] = dbCurrentPerm

        return dataSet

    def collectStats(self, database, fullTable):
        '''
        Collects Stats for a Given Table
        '''

        # Get the primary index for the table
        table     = fullTable[fullTable.find('.')+1:]
        indexData = self.getIndexes(database, table, ['P','Q'])
        primaryIndexColumns = []
        for idx in indexData:
            primaryIndexColumns.append(idx['columnname'])
        primaryIndex = "\"%s\"" % ("\",\"".join(primaryIndexColumns))

        sql = "COLLECT STATS ON %s column(%s)" % (fullTable, primaryIndex)
        logging.info(sql)
        resultSet = self.executeDDL(database, sql)
        return resultSet

    def createTable(self, dbName, sourceTable, targetTable, withData=False):
        '''
        Creates a table from an existing table (primarily needed for generating temp tables)
        '''
        withDataClause = 'WITH NO DATA'
        if (withData):
           withDataClause = 'WITH DATA'
        sql = "CREATE TABLE %s%s.%s AS %s%s.%s %s" % (self.environmentId, dbName, targetTable, self.environmentId, dbName, sourceTable, withDataClause)
        logging.info(sql)
        self.executeDDL(dbName, sql)


    def callMultiLoad(self):
        '''
        Calls the multiload function.

        USAGE   : etl.py --action=callMultiLoad
        REPLACES: /red_batch/common/cmd/callmload
        '''

        mloadCmd   = " /opt/teradata/client/bin/mload -c KANJISJIS_0S %s" % (mloadFile)
        returnCode = self._executeCommand(mloadCmd)
        return returnCode

    def checkTeradataMaintenance(self):
        file = self.config.get("MAINTENANCE" , "file")
        if os.path.exists(file):
            return RC_ERROR
        return RC_NO_ERROR


    def dbSchemaCheck(self):
        '''
        USAGE       : etl.py --action=dbSchemaCheck
        REPLACES    : /red_batch/common/cmd/db_schema_chk.sh
        TODO        : finish porting the .sh script
        '''

        # Read Password File
        # . ${RED_TERA_PASS}/.tera_security_log_pass

        self._validateOptions(['schema_name','tablekind'], {})
        select =    """
                    SELECT 'show ${SCHEMA_NAME} '||TRIM(databasename)||'.'||TRIM(tablename)
                    FROM dbc.tables
                    (databasename NOT LIKE 'VRF%')
                    AND databasename NOT IN ('DBC','SYSADMIN','SYSTEMFE','SYSUDTLIB')
                    AND (databasename,tablename) IN (SELECT databasename,tablename FROM dbc.tables WHERE tablekind = '%s')
                    """ % (options.tablekind)
        self.db_cursor.execute(select)
        resultSet = self.db_cursor.fetchall()


    def dbSpaceCheck(self):
        '''
        Checkes space in the database

        USAGE       : etl.py --action=dbSpaceCheck
        REPLACES    : /red_batch/common/cmd/db_space_check.sh

        '''
        # Read Password File
        # . ${RED_TERA_PASS}/.tera_security_log_pass

        select =    """
                    SELECT  count(1)
                    FROM    (
                            SELECT  trim(databasename) as dbnm              ,
                                    SUM(currentperm) AS sum_currentperm     ,
                                    SUM(maxperm) AS sum_maxperm             ,
                                    SUM(peakperm) AS sum_peakperm           ,
                                    CASE WHEN sum_currentperm = 0 THEN 0
                                         ELSE CAST((sum_currentperm/sum_maxperm * 100) AS DEC (5,1))
                                    END AS usage
                            FROM    dbc.diskspace
                            WHERE   databasename not like 'DEV_%'
                                    and databasename not like 'VRF_%'
                                    and databasename not like 'UA_%'
                            GROUP BY 1
                            ORDER BY 1
                            )
                    """


    def dropIndexes(self, database, table, indexTypes=[]):
        '''
        Drops any specified detected indexes
        '''
        actualDatabase = "%s%s" % (self.environmentId, database)

        indexData          = self.getIndexes(database, table, indexTypes)
        indexFields        = []
        currentIndexNumber = None
        for index in indexData:
            if currentIndexNumber == None:
               indexFields        = [index['columnname']]
               currentIndexNumber = index['indexnumber']
            elif currentIndexNumber != index['indexnumber']:
               dropStmt           = "DROP INDEX(%s) ON %s.%s" % (','.join(indexFields),actualDatabase,table)
               logging.info(dropStmt)
               self.executeDDL(database, dropStmt)
               indexFields        = [index['columnname']]
               currentIndexNumber = index['indexnumber']
            else:
               indexFields.append(index['columnname'])

        dropStmt           = "DROP INDEX(%s) ON %s.%s" % (','.join(indexFields),actualDatabase,table)
        logging.info(dropStmt)
        self.executeDDL(database, dropStmt)


    def dropTable(self, dbName, tableName):
        '''
        Drops a given table from the database
        '''
        sql = "DROP TABLE %s%s.%s" % (self.environmentId, dbName, tableName)
        logging.info(sql)
        conn = self.executeDDL(dbName, sql)

    def executeFastExport(self):
        '''
        Execute the 'fexp' teradata utility
        '''
        # Validate Command Line Options
        self._validateOptions(['filename'],{})

        # Determine the max sessions that will be used
        if (self.options.max_sessions == None):
           maxSessions = self.config.get(SECTION_RED_TERADATA, 'default_max_sessions')
           msg = "The 'max_sessions' command line option was not set. Using the default value of '%s' " % (maxSessions)
           logging.debug(msg)
        else:
           maxSessions = self.options.max_sessions
           msg = "Max Sessions = '%s'" % (maxSessions)
           logging.debug(msg)

        returncode = self._executeFastExport(self.options.filename, maxSessions)
        return returncode

    def getFastloadErrorTables(self, database, errorTable, uniqueViolationTable, maxRows=200):
        '''
        Returns the row counts, and 0:maxData rows of data.
        '''
        errorInfo = {'error_table_count': 0       , 'unique_violation_count': 0 ,
                      'error_table_data' : None    , 'unique_violation_data' : None}
        topClause = ""
        if (maxRows):
           topClause = "top %s" % (int(maxRows))

        if (self.hasTable(database, errorTable)):
            tableName                          = "%s%s.%s" % (self.environmentId, database, errorTable)
            sql                                = "locking %s for access select count(1) from %s" % (tableName, tableName)
            logging.info(sql)
            resultSet                          = self.executeSql(database, sql)
            count                              = resultSet[0][0]
            errorInfo['error_table_count']     = count

            tableName                     = "%s%s.%s" % (self.environmentId, database, errorTable)
            sql                           = "locking %s for access select %s * from %s" % (tableName, topClause, tableName)
            #logging.info(sql)
            #resultSet                     = self.executeSql(database, sql)
            #errorInfo['error_table_data'] = resultSet

        if (self.hasTable(database, uniqueViolationTable)):
            tableName                           = "%s%s.%s" % (self.environmentId, database, uniqueViolationTable)
            sql                                 = "locking %s for access select count(1) from %s" % (tableName, tableName)
            logging.info(sql)
            resultSet                           = self.executeSql(database, sql)
            count                               = resultSet[0][0]
            errorInfo['unique_violation_count'] = count

            tableName                          = "%s%s.%s" % (self.environmentId, database,uniqueViolationTable)
            sql                                = "locking %s for access select %s * from %s" % (tableName, topClause, tableName)
            #logging.info(sql)
            #resultSet                          = self.executeSql(database, sql)
            #errorInfo['unique_violation_data'] = resultSet

        return errorInfo

    def generateFastExportFile(self):
        '''
        Generates a Fast Export Control File, based on the DDL of the specified table
        '''
        # Validate Command Line Options
        self._validateOptions(['table'],{})

    def getColumnType(self, database, table, column):
        envDbName = self._getActualTeradataSchema(database)
        sql = 'HELP COLUMN %s.%s.%s' % (envDbName, table, column)
        res = self.executeSql(database, sql)
        if not res:
            msg = "Failed to get info of column %s.%s.%s" % (envDbName, table, column)
            raise Exception(msg)
        columnType =  res[0][1].encode('utf8').strip()
        logging.debug("column type is %s" % columnType)
        return columnType

    def getColumnNames(self, database, table):
        columnInfos = self.getTableDefinition(database, table)
        columnNames = []
        for columnInfo in columnInfos:
            columnNames.append(columnInfo[COLUMN_NAME].strip().encode('utf8'))
        return columnNames

    def getTableDefinition(self, schema, tablename):
        '''
        Runs HELP TABLE to get a tables definition
        '''
        actualSchema         = self._getActualTeradataSchema(schema)
        tableDefSql          = 'HELP TABLE %s.%s' % (actualSchema, tablename)
        tableDefinition      = self.executeSql(schema, tableDefSql, '')
        cleanTableDefinition = []
        for row in tableDefinition:
            defn = {'column_name':row[0]             , 'type':row[1].strip()         , 'comment':row[2]                   ,
                    'nullable':row[3]                , 'format':row[4]               , 'title':row[5]                     ,
                    'max_length':row[6]              , 'decimal_total_digits':row[7] , 'decimal_fractional_digits':row[8] ,
                    'range_low':row[9]               , 'range_high':row[10]          , 'uppercase':row[11]                ,
                    'table_view':row[12]             , 'default_value': row[13]      , 'char_type':row[14]                ,
                    'idcol_type':row[15]             , 'udt_name':row[16]            , 'temporal': row[17]                ,
                    'column_dictionary_name':row[18] , 'column_sql_name':row[19]     , 'column_name_uescape':row[20]      ,
                    'dictionary_title':row[21]       , 'sql_title': row[22]          , 'title_uescape':row[23]            ,
                    'udt_dictionary_name':row[24]    , 'udt_sql_name': row[25]       , 'udt_name_uescape':row[26]}
            cleanTableDefinition.append(defn)

        return cleanTableDefinition

    def getTablePrimaryIndex(self, database, table):
        envDbName = self._getActualTeradataSchema(database)
        sql = 'HELP COLUMN %s.%s.*' % (envDbName, table)
        res = self.executeSql(database, sql)
        primaryIndex = []
        for row in res:
            if row[11].encode('utf8').strip() == 'Y' and row[13].encode('utf8').strip() == 'P':
                primaryIndex.append(row[32].encode('utf8').strip())
        logging.debug("Primary Index of %s.%s is %s" % (envDbName, table, primaryIndex))
        return primaryIndex

    def getTableUniquePrimaryIndex(self, database, table):
        envDbName = self._getActualTeradataSchema(database)
        sql = 'HELP COLUMN %s.%s.*' % (envDbName, table)
        res = self.executeSql(database, sql)
        primaryIndex = []
        for row in res:
            if row[11].encode('utf8').strip() == 'Y' and row[12].encode('utf8').strip() == 'Y' and row[13].encode('utf8').strip() == 'P':
                primaryIndex.append(row[32].encode('utf8').strip())
        logging.debug("Unique Primary Index of %s.%s is %s" % (envDbName, table, primaryIndex))
        return primaryIndex

    def executeSql(self, dbName, sql, parameters=None, isolation=0, fetch=100, convertToHash=False):
        """
        The following ways are used for improving performance
            -fetchmany

            -isolation='READ UNCOMMITTED'
                '': use default
                'READ UNCOMMITTED'
                'SERIALIZABLE'
        """
        #TODO: replace with _getJDBC()
        (rc, variables) = self._parseTeraPass(dbName)
        envJdbcSection  = "%s_%s" % (self.environment, SECTION_JDBC)
        url             = self.config.get(envJdbcSection, CONNECT)
        driver          = self.config.get(envJdbcSection, DRIVER)
        username        = variables['USER_LOGON'].replace("\n", "").replace("\"", "")
        password        = variables['PASSWORD'].replace("\n", "").replace("\"", "")
        #logging.debug('JDBC URL=%s' % url)
        # Parameters not included
        logging.debug('SQL=\n%s' % sql)
        jdbc = None
        try:
            if isolation==0:
                isolation=''
            if isolation==1:
                isolation='READ UNCOMMITTED'
            if isolation==2:
                isolation='READ UNCOMMITTED'
            if isolation==3:
                isolation='SERIALIZABLE'
            if isolation==4:
                isolation='SERIALIZABLE'

            jdbc = JDBC(url, driver, username, password,isolation)

            # Get Database Cursor
            cursor = jdbc.execute(sql, parameters)

            # Execute SQL
            result_list = []

            # Retrieve the result set
            if (cursor._rs is not None):
                records = cursor.fetchmany(fetch, convertToHash)
                while records:
                    for record in records:
                        result_list.append(record)
                    records = cursor.fetchmany(fetch, convertToHash)

            # Close Database Cursor
            cursor.close()

        except Exception, msg:
            logging.error(msg)
            raise
        return result_list

    def executeDDL(self, dbName, sql,isolation=0):
        """
        Executes a SQL(INSERT , DELETE etc.) through JDBC.

        Args:
            dbName: teradata database name
            sql: SQL

        Returns:
            result code

        Raises:
            Exception: database connection error
        """
        (rc, variables) = self._parseTeraPass(dbName)
        envJdbcSection  = "%s_%s" % (self.environment, SECTION_JDBC)
        url             = self.config.get(envJdbcSection, CONNECT)
        driver          = self.config.get(envJdbcSection, DRIVER)
        username        = variables['USER_LOGON'].replace("\n", "").replace("\"", "")
        password        = variables['PASSWORD'].replace("\n", "").replace("\"", "")
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
                isolation='READ UNCOMMITTED'
            if isolation==3:
                isolation='SERIALIZABLE'
            if isolation==4:
                isolation='SERIALIZABLE'
            jdbc = JDBC(url, driver, username, password,isolation)

            # Execute SQL
            result = jdbc.execute(sql, None)

        except Exception, msg:
            logging.error(msg)
            raise
        return result

    def executeInsert(self, dbName, insertStmt, parameters=None):
        '''
        Inserts data and returns the id of the last inserted record
        '''
        (rc, variables) = self._parseTeraPass(dbName)
        envJdbcSection  = "%s_%s" % (self.environment, SECTION_JDBC)
        url             = self.config.get(envJdbcSection, CONNECT)
        driver          = self.config.get(envJdbcSection, DRIVER)
        username        = variables['USER_LOGON'].replace("\n", "").replace("\"", "")
        password        = variables['PASSWORD'].replace("\n", "").replace("\"", "")
        logging.debug('JDBC URL=%s' % url)

        # Parameters not included
        logging.debug('Insert=%s' % insertStmt)

        jdbc            = JDBC(url, driver, username, password)
        lastGeneratedID = jdbc.insert(insertStmt)

        return lastGeneratedID

    def insertFromTemp(self, database, tmpTable, table, primaryKey, mode=DEFAULT_MODE):
        '''
        Inserts data from the tmpTable into table, via its primary key
        '''
        if (mode == MODE_FULL):
            sql = "DELETE FROM %s%s.%s" % (self.environmentId, database, table)
        else:
            sql = "DELETE FROM %s%s.%s WHERE (%s) IN (SELECT %s FROM %s%s.%s)" % (self.environmentId, database, table, primaryKey, primaryKey, self.environmentId, database, tmpTable)
        logging.info(sql)
        self.executeDDL(database, sql)
        sql = "INSERT INTO %s%s.%s SELECT * FROM %s%s.%s" % (self.environmentId, database, table, self.environmentId, database, tmpTable)
        logging.info(sql)
        self.executeDDL(database, sql)

    def getIndexes(self,database, table, indexTypes=[],account=None):
        """
        Get Indexes for the given table

        Args:
            account        : For getting a password file
        """
        # Determine the Actual Database
        actualDatabase = "%s%s" % (self.environmentId, database)

        # Create Index Type Where Clause
        indexTypeClause = ""
        if (len(indexTypes) > 0):
           inClause        = "'%s'"              % ("','".join(indexTypes))
           indexTypeClause = "AND indextype in (%s)" % (inClause)

        # Generate the query and execute it
        sql = """
              SELECT  databasename , tablename   , columnname     ,
                      indextype    , indexnumber , columnposition ,
                      indexname
              FROM   dbc.indicesV
              WHERE  tablename='%s'
                     AND databasename='%s'
                     %s
              GROUP BY databasename , tablename   , columnname     ,
                       indextype    , indexnumber , columnposition ,
                       indexname
              ORDER BY databasename , indexnumber , columnposition
              """ % (table, actualDatabase, indexTypeClause)


        if account:
            resultSet = self.executeSql(account,sql)
        else:
            resultSet = self.executeSql(database,sql)

        # Process the Results
        indexData = []
        for row in resultSet:
            index = {'databasename':row[0].strip() , 'tablename':row[1]  , 'columnname':row[2].strip(),
                     'indextype'   :row[3] , 'indexnumber':row[4], 'columnposition':row[5],
                     'indexname'   :row[6]}
            indexData.append(index)
        return indexData

    def getDatabaseList(self):
        """
        Get DB list except for system databases(TERADATA_OMIT_DBLIST1+TERADATA_OMIT_DBLIST2)

        Args:

        Returns:
            dbList: adbook,affiliate,bi etc (without dev , lower case)

        """
        dbList=[]

        sql   = ""
        sql  += "select databasename from dbc.diskspace \n"
        sql  += "where 1=1 \n"

        #pro or stg
        if self.environment == PRODUCTION_ENVIRONMENT:
            sql  += "and trim(databasename) not like 'DEV@_%'  ESCAPE '@'  \n"
        else:
            sql  += "and trim(databasename) like 'DEV@_%'  ESCAPE '@'  \n"

        #omit system databases
        allList=TERADATA_OMIT_DBLIST1+TERADATA_OMIT_DBLIST2
        for tmp in allList:
            if '%' in tmp:
                if '_' in tmp:
                    tmp=tmp.replace('_','@_')
                    sql  += "and trim(databasename) not  like '%s' ESCAPE '@' \n" % tmp
                else:
                    sql  += "and trim(databasename) not  like '%s' \n" % tmp
            else:
                sql  += "and trim(databasename) <> '%s' \n" % tmp

        sql  += "group by 1 \n"
        sql  += "order by databasename "

        #execute sql
        #logging.info(sql)
        recordSet = self.executeSql(TERADATA_READ_USER,sql,'')

        for rec in recordSet:
            dbname      = rec[0].encode('utf8').strip()
            dbname=dbname.replace('DEV_','')
            dbname=dbname.lower()
            dbList.append(dbname)

        return dbList

    def getTableList(self,schema,account=None):
        """
        Get table list

        Args:
            schema         : logical schema
            account        : For getting a password file
        Returns:
             tableList
        """
        # Set the account if it is not defined
        if account == None:
           account = schema

        # Generate SQL
        sql   = """
                SELECT   tablename , tablekind
                FROM     dbc.TablesV
                WHERE    databasename='%s'
                ORDER BY tablename
                """ % (schema)

        # Process Results
        tableList = []
        recordSet = self.executeSql(account,sql,'')

        for rec in recordSet:
            tablename      = rec[0].encode('utf8').strip()
            tablekind      = rec[1].encode('utf8').strip()
            if tablekind=='T':
                tableList.append(tablename)

        return tableList


    def hasTable(self, schema, tableName,account=None):
        """
        Check table existence through JDBC.

        Args:
            schema     : logical schema
            tableName  : table name
            account    : For getting a password file

        Returns:
            hasTable : True or False
        """
        hasTable        = True
        actualSchema    = self._getActualTeradataSchema(schema)

        # Create JDBC object
        if account:
            (rc, variables) = self._parseTeraPass(account)
        else:
            (rc, variables) = self._parseTeraPass(schema)

        envJdbcSection  = "%s_%s" % (self.environment, SECTION_JDBC)
        url             = self.config.get(envJdbcSection, CONNECT)
        driver          = self.config.get(envJdbcSection, DRIVER)
        username        = variables['USER_LOGON'].replace("\n", "").replace("\"", "")
        password        = variables['PASSWORD'].replace("\n", "").replace("\"", "")
        jdbc            = JDBC(url, driver, username, password)
        logging.debug('JDBC URL=%s' % url)

        # Query data dictionary if table exists
        sql = "select 1 from dbc.TablesV where databasename ='%s' and TableName = '%s'" % (actualSchema,tableName)
        logging.debug('SQL=%s' % sql)
        cursor = jdbc.execute(sql, None)

        # Return True or False depending on if table exists
        record = cursor.fetchone()
        if record is None:
           hasTable = False

        return hasTable

    def getCLOBInfo(self, schema_teradata,table_teradata, tableDefinition=None):
        """
        Modify columns to columns sorted by CLOB columns

        Args:
            tableDefinition

        Returns:
            clobCount                :the number of CLOB coumns in the table
            clobDefs                 :Definition of CLOB coumns
            tableDefinition sorted   :Definition of ALL coumns (CLOB coumns are in first part)
        """
        clobCount               = 0
        clobDefs                = []
        tableDefinitionSorted   = []

        try:
            #get Teradata table
            actual_schema = self._getActualTeradataSchema(schema_teradata)

            # Check table existence
            exist_flg = self.hasTable(schema_teradata, table_teradata)
            if exist_flg == False:
                # table_teradata may be 'review_data_20%%' etc.
                # But at this time, it could be considered as Non CLOB table.
                logging.warn('Check CLOB table or not : Table does not exist. %s/%s' % (schema_teradata, table_teradata) )
                return (clobCount,clobDefs,tableDefinition)

            # Get Definition
            tableDefSql = 'HELP TABLE %s.%s' % (actual_schema, table_teradata)
            tableDefinitionTera = self.executeSql(schema_teradata, tableDefSql, '')

            # Get clobCount
            for definition_tera in tableDefinitionTera:
                #Check if CLOB with Teradata table
                def_TypeSize = definition_tera[1].strip().encode('utf8')
                if def_TypeSize == 'BO' or def_TypeSize == 'CO':
                    clobCount+=1

            # Get tableDefinitionSorted
            if clobCount==0 or tableDefinition is None:
                tableDefinitionSorted=tableDefinition

            else:
                col_num     =len(tableDefinition)
                col_num2    =len(tableDefinitionTera)
                if col_num!=col_num2:
                    logging.error('Check CLOB table or not : The number of columns is different. %s/%s' % (schema_teradata, table_teradata) )
                    return (clobCount,clobDefs,tableDefinition)

                otherDefs   =[]
                for i in range(col_num):
                    definition      =tableDefinition[i]
                    definition_tera =tableDefinitionTera[i]

                    #Check if CLOB with Teradata table
                    def_TypeSize = definition_tera[1].strip().encode('utf8')
                    if def_TypeSize == 'BO':
                        definition['BLOB']  = True
                        clobDefs.append(definition)
                    elif def_TypeSize == 'CO':
                        definition['CLOB']  = True
                        clobDefs.append(definition)
                    else:
                        definition['BLOB']  = False
                        definition['CLOB']  = False
                        otherDefs.append(definition)

                tableDefinitionSorted=[]
                tableDefinitionSorted.extend(clobDefs)

                for definition in otherDefs:
                    tableDefinitionSorted.append(definition)

        except Exception, msg:
            logging.error(str(msg))
            clobCount   =0

        return (clobCount,clobDefs,tableDefinitionSorted)

    def recreateIndexes(self, database, table, indexData):
        '''
        Drops any specified detected indexes
        '''
        actualDatabase = "%s%s" % (self.environmentId, database)

        indexFields        = []
        indexNames         = {}
        currentIndexNumber = None
        for index in indexData:
            indexNames[index['indexnumber']] = index['indexname']
            if currentIndexNumber == None:
               indexFields             = [index['columnname']]
               currentIndexNumber      = index['indexnumber']
            elif currentIndexNumber   != index['indexnumber']:
               createStmt              = "CREATE INDEX(%s) ON %s.%s" % (','.join(indexFields),actualDatabase,table)
               #self.executeDDL(database, createStmt)
               indexFields           = [index['columnname']]
               currentIndexNumber    = index['indexnumber']
            else:
               indexFields.append(index['columnname'])

        createStmt           = "CREATE INDEX(%s) ON %s.%s" % (','.join(indexFields),actualDatabase,table)
        #self.executeDDL(database, createStmt)

    def createBteqFile(self, template, parameters):
        # Set path and filename for the BTEQ control file
        fileName = '%s_%s.btq' % (template, str(uuid.uuid4()))
        bteqFile = os.path.join(self.getLogDir(), fileName)
        # Generate BTEQ control file
        self.createFileFromTemplate(template, parameters, bteqFile)
        return bteqFile

    def callBteq(self, bteqFile):
        # Execute bteq file
        bteqCmd = "bteq < %s" % bteqFile
        logging.info("Now executing BTEQ...")
        (stdin, stdout, stderr, returncode) = self._executeOsCommand(bteqCmd)
        logging.info("Finished BTEQ execution.")
        # Deal with return code
        if returncode != RC_BTEQ_NORMAL:
            msg = "Bteq file '%s' execution failed with return code %d." % (bteqFile, returncode)
            logLevel = 'info'
        else:
            msg = "Bteq file '%s' execution finished successfully." % (bteqFile)
            logLevel = 'debug'
            os.unlink(bteqFile)
            logging.info("The BTEQ control file '%s' was removed." % (bteqFile))
        self._log(msg,stdout,stderr,logLevel)
        return returncode

    def getTeradataLogonInfo(self, database):
        teraPassInfo = self._parseTeraPass(database)[1]
        username = teraPassInfo[TERA_PASS_USER_LOGON].replace('"','').replace('\n','')
        password = teraPassInfo[TERA_PASS_PASSWORD].replace('"','').replace('\n','')
        tdpid = self.config.get(SECTION_TERADATA , TDPID)
        return username, password, tdpid

    def _getParametersForReleasingTable(self, database, table):
        parameters = {}
        parameters[DATABASE] = database
        parameters[ENV_DBNAME] = self._getActualTeradataSchema(database)
        parameters[TABLE] = table
        parameters[USERNAME], parameters[PASSWORD], parameters[TDPID] = self.getTeradataLogonInfo(database)
        parameters[CURRENT_DATE] = datetime.today().strftime('%Y%m%d')
        parameters[CURRENT_DATETIME] = datetime.today().strftime('%Y%m%d%H%M')
        return parameters

    def readDDLFromFile(self, database, table):
        file = self.getDDLFile('teradata', database, table)
        fileBody = open(file, 'r').read()
        return fileBody

    def updateSchemeFromDDLfile(self, database, table):
        fullTableName = self.environmentId + database + '.' + table
        logging.info("Start updating table: %s" % fullTableName)
        parameters = self._getParametersForReleasingTable(database, table)
        parameters[TABLE_DEF] = self.readDDLFromFile(database, table)
        bteqFile = self.createBteqFile(TERADATA_UPDATE_SCHEME_TEMPLATE_FILE, parameters)
        returnCode = self.callBteq(bteqFile)
        if returnCode != RC_BTEQ_NORMAL:
            logging.error("Updating '%s' failed with return code %d." % (fullTableName, returnCode))
            return RC_ERROR
        else:
            logging.info("The table '%s' was updated successfully." % (fullTableName))
            return RC_NO_ERROR

    def isExpiredBackupTable(self, tableName, daysBack=0):
        suffix = tableName.split('_')[-1]
        backupDate = self.getDatetimeFromString(suffix, 'YYYYMMDD')
        if backupDate is not None and datetime.now() - backupDate > timedelta(days=int(daysBack)):
            return True
        else:
            return False

    def deleteTable(self, database, table):
        fullTableName = self.environmentId + database + '.' + table
        logging.info("Start deleting table: %s" % fullTableName)
        parameters = self._getParametersForReleasingTable(database, table)
        bteqFile = self.createBteqFile(TERADATA_DELETE_TABLE_TEMPLATE_FILE, parameters)
        returnCode = self.callBteq(bteqFile)
        if returnCode != RC_BTEQ_NORMAL:
            logging.error("Deleting '%s' failed with return code %d." % (fullTableName, returnCode))
            return RC_ERROR
        else:
            logging.info("The table '%s' was deleted successfully." % (fullTableName))
            return RC_NO_ERROR

    def deleteBackupTable(self, database, table, daysBack=0):
        if not self.isExpiredBackupTable(table, daysBack):
            logging.warning("Skipped deleting table '%s'. It is not a backup table or not expired yet." % table)
            return RC_DELETE_SKIPPED
        return self.deleteTable(database, table)

    def getBackupTables(self, database):
        envDbName = self._getActualTeradataSchema(database)
        sql = "SELECT tablename FROM dbc.tablesV WHERE databasename = '%s' AND REGEXP_SIMILAR(tablename, '.*_[0-9]{8}$', 'i') = 1" % envDbName
        res = self.executeSql(database, sql)
        tables = [row[0].encode('utf8').strip() for row in res]
        logging.info("Got backup tables: %s" % tables)
        return tables

    def getExpiredBackupTables(self, database, daysBack=0):
        backupTables = self.getBackupTables(database)
        if not daysBack:
            return backupTables
        return [table for table in backupTables if self.isExpiredBackupTable(table, daysBack)]
