import logging
import os.path
import uuid
from datetime import datetime, timedelta

from modules.base.constants    import *
from modules.base.environment  import Environment
from modules.teradata.db       import TeradataDB

class Fastload(TeradataDB):
    '''
    This class for all fastload related functionality
    '''
    def __init__(self):
        super(Fastload, self).__init__()
        self.config.read('conf/modules/teradata/fastload.cfg')

    #####################
    # Private Functions #
    #####################
    def _generateFastloadColumnDef(self, column):
        columnName = column[COLUMN_NAME].strip().encode('utf8')
        columnType = column[TYPE].strip().encode('utf8')
        columnMaxLength = column[MAX_LENGTH]
        if columnType in TERADATA_CHAR_TYPES:
            columnDef = '%s (VARCHAR(%s))' % (columnName, (int(columnMaxLength)>>1)*3)
        elif columnType in TERADATA_NUMBER_TYPES:
            columnDef = '%s (VARCHAR(20))' % (columnName)
        elif columnType in TERADATA_DATETIME_TYPES:
            columnDef = '%s (VARCHAR(30))' % (columnName)
        else:
            msg = "BYTE/VARBYTE/LOB types are not supported by fastload yet!"
            raise Exception(msg)
        return columnDef

    ####################
    # Public Functions #
    ####################
    def callBteq(self, bteqFile):
        '''
        Calls bteq utility
        Args:
        bteqFile : Path of bteq file
        '''
        # Execute BTEQ
        bteqCmd = "bteq < %s" % bteqFile
        logging.info("Now executing BTEQ...")
        (stdin, stdout, stderr, returncode) = self._executeOsCommand(bteqCmd)
        logging.info("Finished BTEQ execution.")

        # Log BTEQ Output and parse the results
        results = {}
        results[RETURN_CODE] = returncode
        for line in stdout:
            logging.info(line)
            if (line.find('Delete completed.') != -1):
                try:
                    value = int(line.split('.')[1].strip().split(' ')[0])
                except Exception as e:
                    logging.error(e)
                    logging.error("Error when parsing line\n'%s':" % line)
                    continue
                results[DELETE_COUNT] = value
                continue
            if (line.find('Insert completed.') != -1):
                try:
                    value = int(line.split('.')[1].strip().split(' ')[0])
                except Exception as e:
                    logging.error(e)
                    logging.error("Error when parsing line\n'%s':" % line)
                    continue
                results[INSERT_COUNT] = value
                continue

        # Log any Errors Generated
        for line in stderr:
            logging.info(line)

        return results

    def callFastload(self, characterSet, fastloadFile, clobCount=0):
        '''
        Calls the fastloador or bteq utility for Fastload up to whether clobCount=0 or not.

        Args:
        fastloadFile    :Path of fastloadFile
        clobCount       :The number of CLOB columns in the table
        '''
        results = {}
        if clobCount==0:
            fastloadExePath = self._readMandatoryOption(SECTION_OS_COMMANDS, FASTLOAD_COMMAND)
        else:
            fastloadExePath = self.config.get(SECTION_TERADATA, 'BTEQ_COMMAND')

        # Execute Fastload
        fastloadCmd = "%s -c %s < %s" % (fastloadExePath, characterSet, fastloadFile)
        logging.info("Now loading by fastload...")
        (stdin, stdout, stderr, returncode) = self._executeOsCommand(fastloadCmd)
        logging.info("Finished loading.")
        results[RETURN_CODE] = returncode

        # Log Fastload Output and parse the results
        for line in stdout:
            logging.info(line)
            if (line.find('Total Records Read') != -1):
                value = int(line.split('=')[1].strip())
                results['total_records_read'] = value
                continue
            if (line.find('Total Error Table 1') != -1):
                value = int(line.split('=')[1].strip().split(' ')[0])
                results[ET_COUNT] = value
                continue
            if (line.find('Total Error Table 2') != -1):
                value = int(line.split('=')[1].strip().split(' ')[0])
                results[UV_COUNT] = value
                continue
            if (line.find('Total Inserts Applied') != -1):
                value = int(line.split('=')[1].strip())
                results['total_inserts_applied'] = value
                continue
            if (line.find('Total Duplicate Rows') != -1):
                value = int(line.split('=')[1].strip())
                results['total_duplicate_rows'] = value
                continue
        # Log any Errors Generated
        for line in stderr:
            logging.info(line)

        return results

    def createFastloadControlFile(self, characterSet, targetInfo, fileToLoad, tableDefinition,clobCount,tableDefinitionSorted, columnsToLoad):
        '''
        Creates the fastload or bteq control file from a MySQL table definition up to whether clobCount=0 or not.

        Args:
        clobCount                :The number of CLOB columns in the table
        tableDefinitionSorted    :CLOB columns are moved at firtst part on MySQL tableDefinition

        Returns:
        filename: Filename of the control file that can be executed

        Exceptions Thrown:
        None
        '''

        if clobCount==0:
            fastloadTemplateFilename = self._readMandatoryOption(SECTION_FILES, TERADATA_FASTLOAD_TEMPLATE_FILE)
            tableDefinition          = tableDefinition
        else:
            fastloadTemplateFilename = self._readMandatoryOption(SECTION_FILES, TERADATA_BTEQLOAD_TEMPLATE_FILE)
            tableDefinition          = tableDefinitionSorted

        # Create Substitutions
        targetColumns                              = targetInfo[COLUMN_NAMES]
        if (targetColumns != None):
           targetColumns = targetColumns.split(',')
           targetColumns.reverse()
        # Determine ML_JOBNET_NAME
        jobnet_name = 'DEV_SAMPLE'
        if os.environ.has_key('ML_JOBNET_NAME'):
             jobnet_name = os.environ['ML_JOBNET_NAME']
        logging.debug("target info parallel load pass Directory Set to '%s'" % (targetInfo))

        # Add additional data to targetInfo (Maybe will be moved later)
        if targetInfo[PARALLEL_LOAD_PASS] is not None :
            targetPASS= self._parallel_logon(targetInfo[DATABASE],targetInfo[PARALLEL_LOAD_PASS])
            teraPassInfo                            = self._parseTeraPass(targetPASS)[1]
        else:
            teraPassInfo                            = self._parseTeraPass(targetInfo[DATABASE])[1]

        logonuser                               = teraPassInfo['LOGON_USER'].strip()
        logonuser                               = logonuser.replace('\n','')
        logonuser                               = logonuser.replace('"','')
        targetInfo['logon_user']                = logonuser
        targetInfo['loadTable']                 = targetInfo[TMP_TABLE]
        targetInfo['loadErrorTable']            = "%s_et" % (targetInfo[TMP_TABLE])
        targetInfo['loadUniqueViolationsTable'] = "%s_uv" % (targetInfo[TMP_TABLE])

        # Set delimeter
        if clobCount==0:
            delimeter = targetInfo[DELIMETER]
            if (targetInfo[DELIMETER] == 'TAB'):
               setRecord = "set record vartext \"	\""
            else:
               setRecord = "set record vartext \"%s\"" % (targetInfo[DELIMETER])
            targetInfo['setRecordStatement'] = setRecord
        else:
            delimeter       =DELIMETER_TAB
            # Set charset
            if characterSet=='ascii':
                targetInfo['charset'] = FLOAD_CHARSET_ASCII
            else:
                targetInfo['charset'] = FLOAD_CHARSET_KANJISJIS_0S
                #targetInfo['charset'] = FLOAD_CHARSET_KANJIEUC_0U

        # Create preparation statements (DELETE/DROP statements)
        prepStatements  = "SET  QUERY_BAND = 'UTILITYNAME=FASTLOAD;UtilityDataSize=SMALL;JOBNETNAME=" + jobnet_name + ";JOBNAME=" + targetInfo['tmp_table'] +";' FOR SESSION;\n"
        prepStatements += "DROP TABLE  %s;\n" % (targetInfo[TMP_TABLE])
        prepStatements += "CT %s AS  %s WITH NO DATA ;\n" % (targetInfo[TMP_TABLE],targetInfo[TABLE])
        prepStatements += "DROP TABLE %s;\n"  % (targetInfo['loadErrorTable'])
        prepStatements += "DROP TABLE %s;\n"  % (targetInfo['loadUniqueViolationsTable'])
        targetInfo['preparationStatements'] = prepStatements


        # Create the Define and Insert Parts of the Control File
        defineStatement  = "define\n"

        importStatement  = ".IMPORT VARTEXT '%s'  LOBCOLS %s  FILE=\"%s\"\n" % (delimeter,clobCount,fileToLoad)
        importStatement += ".REPEAT *\nUSING (\n"

        # Filter out columns that we do not need to load
        newTableDefinition = []
        columnsToLoadArray = columnsToLoad.split(',')
        for col in tableDefinition:
            column     = col['column_sql_name'].replace('"','')
            if (columnsToLoad == 'all'):
               newTableDefinition.append(col)
            elif (column in columnsToLoadArray):
               newTableDefinition.append(col)
        tableDefinition = newTableDefinition

        # Create the necessary statements for the control file
        insertClause,valuesClause = "",""
        for col in tableDefinition:
           column     = col['column_sql_name'].replace('"','')
           origColumn = col['column_sql_name'].replace('"','')
           if (column in TERADATA_RESERVED_WORDS):
               column = "%sField" % (column)
           elif (self._isInteger(column[0])):
               column = "field_%s" % (column)

           # Append to the Define Statement
           if (col['type'] in ('CV','CF')):
               defineStatement += "\t%s (VARCHAR(%s)),\n" % (column, col['max_length'])
           elif (col['type'] == 'F'):
               defineStatement += "\t%s (VARCHAR(100)),\n" % (column)
           else:
               defineStatement += "\t%s (VARCHAR(30)),\n" % (column)

           # Append to the Import Statement
           if (col['type'] in ('CO','BO')):
               importStatement += "\t%s BLOB AS DEFERRED,\n" % (column)
           elif (col['type'] in ('CV','CF')):
               importStatement += "\t%s VARCHAR(%s),\n" % (column, col['max_length'])
           else:
               importStatement += "\t%s VARCHAR(30),\n" % (column)

           # Append to Insert Clause
           if (targetColumns == None):
               insertClause += "\"%s\",\n"  % (origColumn)
           else:
               insertClause += "\"%s\",\n" % (targetColumns.pop())

           # Append to Values Clause
           valuesClause += ":%s,\n" % (column)

        defineStatement = "%s\nfile=%s;\n" % (defineStatement[:-2], fileToLoad)
        importStatement = "%s\n)\n" % (importStatement[:-2])

        insertStatement = "insert into %s\n(\n%s\n)\nvalues\n(\n%s\n);\n" % (targetInfo['tmp_table'],insertClause[:-2],valuesClause[:-2])
        targetInfo['insertStatement'] = insertStatement

        if (clobCount == 0):
            targetInfo['defineStatement'] = defineStatement
        else:
            targetInfo['importStatement'] = importStatement

        # Create the work directory
        #TODO: Change the work directory
        workDir = self.config.get('BTEQ', 'WORK_DIR')
        logging.debug("Work Directory Set to '%s'" % (workDir))
        self._createWorkDirectory(workDir)

        # Read the Template and substitute in the data
        fastloadTemplateFile = open(fastloadTemplateFilename,'r')
        fastloadTemplate     = fastloadTemplateFile.read()
        actualFastload       = fastloadTemplate % targetInfo

        # Write the actual fastload file to be executed
        filename         = "fastload_%s.fload" % (str(uuid.uuid4()))
        fastloadFilepath = os.path.abspath(os.path.join(workDir, filename))
        logging.debug("Fastload File: '%s'." % (fastloadFilepath))
        actualFastloadFile = open(fastloadFilepath, "w")
        actualFastloadFile.write(actualFastload)
        actualFastloadFile.close()

        return fastloadFilepath

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

    def _parallel_logon(self,dbname,load_path):
        targetPASS=''
        if load_path in {'mysql/sample/analysis'}:
                targetPASS                              = "%s_01" % dbname
        elif load_path in {'mysql/sample/item'}:
                targetPASS                              = "%s_02" % dbname
        elif load_path in {'mysql/sample/merchant' }:
                targetPASS                              = "%s_03" % dbname
        elif load_path in {'mysql/sample/campaign','mysql/sample/canvas'}:
                targetPASS                              = "%s_04" % dbname
        elif load_path in {'mysql/sample/gpoint_id','mysql/sample/gpoint_gb' }:
                targetPASS                              = "%s_05" % dbname
        elif load_path in {'mysql/sample/rcategory','mysql/sample/rating','mysql/sample/mallconfig','mysql/sample/newsletter','mysql/sample/brand','mysql/sample/gpoint_th','mysql/sample/gpoint_tw'}:
                targetPASS                              = "%s_06" % dbname
        elif load_path in {'mysql/sample/order' }:
                targetPASS                              = "%s_07" % dbname
        elif load_path in {'mysql/sample/ibs','mysql/sample/inquiry' ,'mysql/sample/gpoint_sg' }:
                targetPASS                              = "%s_08" % dbname
        elif load_path in {'mysql/sample/my','mysql/sample/inventory' ,'mysql/sample/gold' }:
                targetPASS                              = "%s_09" % dbname
        elif load_path in {'mysql/sample/es','mysql/sample/coupon_lcp' ,'mysql/sample/web_resource' }:
                targetPASS                              = "%s_10" % dbname
        else:
                targetPASS                              =  dbname

        return targetPASS

    def createLoadJobFile(self, sourceDir, filename, delimiter, characterSet, database, table, mode=None, primaryKeyStr=None):
        '''
        Create control files for FASTLOAD and BTEQ loading
        '''
        # Get table definition from Teradata
        parameters = {}
        parameters[TDPID] = 'red'
        parameters[SOURCE_DIRECTORY] = sourceDir
        parameters[FILENAME] = filename
        if delimiter.lower() == 'tab':
            delimiter = '\t'
        parameters[DELIMITER] = delimiter.decode('string_escape')
        parameters[CHARACTER_SET] = characterSet
        parameters[DATABASE] = database
        parameters[ENV_DBNAME] = self.environmentId + database
        parameters[TABLE] = table
        teraPassInfo = self._parseTeraPass(database)[1]
        parameters[USERNAME] = teraPassInfo[TERA_PASS_USER_LOGON].replace('"','').replace('\n','')
        parameters[PASSWORD] = teraPassInfo[TERA_PASS_PASSWORD].replace('"','').replace('\n','')
        parameters[JOBNAME]  = '%s_%s_%s' % (database, table, str(uuid.uuid4()).replace('-',''))
        parameters[TMP_TABLE] = table
        parameters[CURRENT_DATETIME] = datetime.today().strftime('%Y%m%d%H%M')
        helpColumns = self.getTableDefinition(database, parameters[TMP_TABLE])
        if self._isLobTable(helpColumns):
            if filename.split('.')[-1].lower() == 'lob':
                msg = "Fastload does not support LOB type!"
                raise Exception(msg)
            else:
                logging.info("Now loading a lob type table with normal length records.")
                logging.info("temporary table will be created as the same as the %s_nolob table." % (table))
                parameters[TMP_TABLE] = table + '_nolob'
                helpColumns = self.getTableDefinition(database, parameters[TMP_TABLE])
        columnNames = []
        columnDefs = []
        for columnInfo in helpColumns:
            columnNames.append(columnInfo[COLUMN_NAME].strip().encode('utf8'))
            columnDefs.append(self._generateFastloadColumnDef(columnInfo))
        parameters['define_scheme_part'] = ",\n        ".join(columnDefs)
        parameters['insert_into_part']   = ",\n        ".join('"{}"'.format(columnName) for columnName in columnNames)
        parameters['values_part'] = ":" +  ",\n        :".join(columnNames)
        bteqloadTemplate = TERADATA_BTEQLOAD2_TEMPLATE_FILE
        if  mode == MODE_FULL:
            logging.info("Load in FULL mode, all records will be deleted and reloaded.")
            parameters[WHERE_CLAUSE] = ""
            if parameters[TMP_TABLE] == table:
                bteqloadTemplate = TERADATA_BTEQLOAD_FULLMODE_TEMPLATE_FILE
        else:
            if not primaryKeyStr:
                primaryKeys = self.getTableUniquePrimaryIndex(database, table)
                if not primaryKeys:
                    msg = "Please set primary_key for the table %s.%s in configuration file or set mode=full!" % (parameters[ENV_DBNAME], table)
                    raise Exception(msg)
                primaryKeyStr = ','.join(primaryKeys)
                logging.info("Got primary Key info from table: '%s'" % primaryKeyStr)
            parameters[WHERE_CLAUSE] = "WHERE (%s) IN (SELECT %s FROM %s.%s_tmp)" % (primaryKeyStr, primaryKeyStr, parameters[ENV_DBNAME], table)

        # Set path for the control file
        jobnetName = ''
        if os.environ.has_key('ML_JOBNET_NAME'):
            jobnetName = os.environ['ML_JOBNET_NAME']
        baseLogDir = self._readMandatoryOption(SECTION_DIRECTORIES, 'LOG_DIRECTORY')

        # Generate FASTLOAD control file
        fastloadTemplateFile = self.config.get(SECTION_FILES, TERADATA_FASTLOAD2_TEMPLATE_FILE)
        logging.info("Using fastload template file: '%s'" % fastloadTemplateFile)
        fastloadFileBody = open(fastloadTemplateFile,'r').read()
        fastloadFileBody = fastloadFileBody % parameters
        fastloadFileName = 'fastload_%s.ctl' % (parameters[JOBNAME])
        fastloadFile = os.path.join(baseLogDir, jobnetName, fastloadFileName)
        with open(fastloadFile, 'w') as f:
            f.write(fastloadFileBody)
        logging.info("The FASTLOAD control file '%s' was created." % fastloadFile)

        # Generate BTEQ control file
        bteqloadTemplateFile = self.config.get(SECTION_FILES, bteqloadTemplate)
        logging.info("Using bteqload template file: '%s'" % bteqloadTemplateFile)
        bteqloadFileBody = open(bteqloadTemplateFile,'r').read()
        bteqloadFileBody = bteqloadFileBody % parameters
        bteqloadFileName = 'bteqload_%s.ctl' % (parameters[JOBNAME])
        bteqloadFile = os.path.join(baseLogDir, jobnetName, bteqloadFileName)
        with open(bteqloadFile, 'w') as f:
            f.write(bteqloadFileBody)
        logging.info("The BTEQ control file '%s' was created." % bteqloadFile)

        return (fastloadFile, bteqloadFile)

    def loadFromFile(self, parameters):
        '''
        load data from file to table
        '''
        # Create load control file
        (floadFile, btqFile) = self.createLoadJobFile(parameters[SOURCE_DIRECTORY], parameters[FILENAME], parameters[DELIMITER], parameters[CHARACTER_SET], parameters[DATABASE], parameters[TABLE], parameters[MODE], parameters[PRIMARY_KEY])

        # Load into tmp table from file
        fastloadInfo = self.callFastload(parameters[CHARACTER_SET], floadFile)
        for key in fastloadInfo:
            logging.info("%s : %s" % (key, fastloadInfo[key]))
        if fastloadInfo[RETURN_CODE] != RC_FASTLOAD_NORMAL:
            return fastloadInfo[RETURN_CODE]
        os.unlink(floadFile)
        logging.info("The fastload control file '%s' was removed." % (floadFile))

        # Load into target table from tmp table
        bteqInfo = self.callBteq(btqFile)
        for key in bteqInfo:
            logging.info("%s : %s" % (key, bteqInfo[key]))
        if bteqInfo[RETURN_CODE] != RC_BTEQ_NORMAL:
            return bteqInfo[RETURN_CODE]
        os.unlink(btqFile)
        logging.info("The bteq control file '%s' was removed." % (btqFile))

        return fastloadInfo[ET_COUNT] + fastloadInfo[UV_COUNT]
