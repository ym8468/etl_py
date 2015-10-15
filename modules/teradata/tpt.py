import logging
import os.path
import uuid
from datetime import datetime, timedelta

from modules.base.constants    import *
from modules.teradata.db       import TeradataDB

class TPT(TeradataDB):
    '''
    This class for all tpt related functionality
    '''
    def __init__(self):
        super(TPT, self).__init__()
        configFile = os.path.join('conf', 'modules', 'teradata', 'tpt.cfg')
        self.config.read(configFile)

    #####################
    # Private Functions #
    #####################
    def _generateTptColumnDef(self, column):
        columnName = column[COLUMN_NAME].strip().encode('utf8')
        columnType = column[TYPE].strip().encode('utf8')
        columnMaxLength = column[MAX_LENGTH]
        if columnType in TERADATA_CHAR_TYPES:
            columnDef = '"%s" VARCHAR(%s)' % (columnName, columnMaxLength)
        elif columnType in TERADATA_NUMBER_TYPES:
            columnDef = '"%s" VARCHAR(20)' % (columnName)
        elif columnType in TERADATA_DATETIME_TYPES:
            columnDef = '"%s" VARCHAR(30)' % (columnName)
        elif columnType in TERADATA_LOB_TYPES:
            columnDef = '"%s" %s(%s) AS DEFERRED BY NAME' % (columnName, TERADATA_DATA_TYPE_REPRESENTATIONS[columnType], columnMaxLength)
        else:
            msg = "BYTE/VARBYTE types are not supported yet!"
            raise Exception(msg)
        return columnDef

    def _transformTbuildLogFile(self, output):
        '''
        Find tbuild log file path in the output and transform it to readable format by tlogview tool
        '''
        # Find original log file path
        logFile = None
        for line in output:
            if 'Job log' in line:
                logFile = line.split(':')[-1].strip()
                break;
        if not logFile:
            logging.error("tbuild log file path not found!")
            return None

        # Set path for the formatted log file
        jobnetName = ''
        if os.environ.has_key('ML_JOBNET_NAME'):
            jobnetName = os.environ['ML_JOBNET_NAME']
        baseLogDir = self._readMandatoryOption(SECTION_DIRECTORIES, 'LOG_DIRECTORY')
        logFileName = logFile.split('/')[-1].split('.')[0]
        formattedLogFile = "%s/%s/%s.ldrlog" % (baseLogDir, jobnetName, logFileName)

        # Run tlogview command
        cmd = "tlogview -l %s > %s" % (logFile, formattedLogFile)
        (stdin, stdout, stderr, returncode) = self._executeOsCommand(cmd)
        if (returncode != 0):
            msg = "Transform tbuild log format failed with return code %d." % returncode
            raise Exception(msg)
        logging.info("The tbuild formatted log file '%s' was created." % (formattedLogFile))

        # Remove the original log file
        os.unlink(logFile)
        logging.info("The tbuild original output file '%s' was removed." % (logFile))

        return formattedLogFile

    def _checkErrorTables(self, output):
        '''
        Check if there is any error record
        '''
        # Find error table count in output
        etCount = None
        uvCount = None
        insertCount = None
        deleteCount = None
        for line in output:
            if 'Total Rows in Error Table 1' in line:
                etCount = int(line.split(':')[-1].strip())
                logging.info("The et count is %s" % (etCount))
                continue
            if 'Total Rows in Error Table 2' in line:
                uvCount = int(line.split(':')[-1].strip())
                logging.info("The uv count is %s" % (uvCount))
                continue
            if 'Rows Deleted:' in line:
                deleteCount = int(line.split(':')[-1].strip())
                logging.info("The delete count is %s" % (deleteCount))
                continue
            if 'Rows Inserted:' in line:
                insertCount = int(line.split(':')[-1].strip())
                logging.info("The insert count is %s" % (insertCount))
                continue
        if etCount is None or uvCount is None:
            logging.error("et/uv count not found!")
            if insertCount is None or deleteCount is None:
                logging.error("insert/delete count not found!")
                return RC_ERROR
            return RC_NO_ERROR
        return (etCount + uvCount)

    ####################
    # Public Functions #
    ####################
    def createLoadJobFile(self, sourceDir, filename, delimiter, characterSet, database, table, mode=None, primaryKeyStr=None):
        '''
        Create job file for TPT loading
        '''
        # Get table definition from Teradata
        parameters = {}
        parameters[TDPID] = 'red'
        parameters[SOURCE_DIRECTORY] = sourceDir
        parameters[FILENAME] = filename
        parameters[DELIMITER] = delimiter.decode('string_escape')
        if parameters[DELIMITER] == '\t':
            parameters[DELIMITER] = 'TAB'
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
        tbuildTemplate = TERADATA_TPTLOAD_TEMPLATE_FILE
        if self._isLobTable(helpColumns):
            if filename.split('.')[-1].lower() == 'lob':
                logging.info("Now loading a lob type table with over length records.")
                tbuildTemplate = TERADATA_TPTINSERTER_TEMPLATE_FILE
            else:
                logging.info("Now loading a lob type table with normal length records.")
                logging.info("temporary table will be created as the same as the %s_nolob table." % (table))
                parameters[TMP_TABLE] = table + '_nolob'
                helpColumns = self.getTableDefinition(database, parameters[TMP_TABLE])
        columnNames = []
        columnDefs = []
        for columnInfo in helpColumns:
            columnNames.append(columnInfo[COLUMN_NAME].strip().encode('utf8'))
            columnDefs.append(self._generateTptColumnDef(columnInfo))
        parameters['define_scheme_part'] = ",\n        ".join(columnDefs)
        parameters['insert_into_part'] = ",\n                ".join('"{}"'.format(columnName) for columnName in columnNames)
        parameters['values_part'] = ":" + ",\n                :".join(columnNames)
        if  mode == MODE_FULL:
            logging.info("Load in FULL mode, all records will be deleted and reloaded.")
            parameters[WHERE_CLAUSE] = ""
            if tbuildTemplate == TERADATA_TPTLOAD_TEMPLATE_FILE and parameters[TMP_TABLE] == table:
                tbuildTemplate = TERADATA_TPTLOAD_FULLMODE_TEMPLATE_FILE
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

        # Generate TPT control file
        jobTemplateFile = self.config.get(SECTION_FILES, tbuildTemplate)
        logging.info("Using tpt load job template file: '%s'" % jobTemplateFile)
        jobFileBody = open(jobTemplateFile,'r').read()
        jobFileBody = jobFileBody % parameters
        jobFileName = 'tptload_%s.ctl' % (parameters[JOBNAME])
        jobFile = os.path.join(baseLogDir, jobnetName, jobFileName)
        with open(jobFile, 'w') as f:
            f.write(jobFileBody)
        logging.info("The TPT control file '%s' was created." % jobFile)

        return jobFile

    def createLoadCmd(self, jobFile, jobName):
        '''
        Create TPT load command
        '''
        cmd = "tbuild -f %s -j %s" % (jobFile, jobName)
        return cmd

    def loadFromFile(self, parameters):
        '''
        load data from file to table
        '''
        # Create load job file
        jobFile = self.createLoadJobFile(parameters[SOURCE_DIRECTORY], parameters[FILENAME], parameters[DELIMITER], parameters[CHARACTER_SET], parameters[DATABASE], parameters[TABLE], parameters[MODE], parameters[PRIMARY_KEY])

        # Create load command
        jobName = os.path.basename(jobFile).split('.')[0]
        cmd = "tbuild -f %s -j %s" % (jobFile, jobName)

        # Execute load command
        logging.info("Now loading by tbuild...")
        (stdin, stdout, stderr, returncode) = self._executeOsCommand(cmd)
        logging.info("tbuild execution finished.")

        # Output command log
        if (returncode != 0):
            msg = "Load failed with return code %d." % returncode
            logLevel = 'info'
        else:
            msg = "The file '%s' was loaded successfully." % (parameters[FILENAME])
            logLevel = 'debug'
            # Delete job file
            os.unlink(jobFile)
            logging.info("The job file '%s' was removed." % (jobFile))
        self._log(msg,stdout,stderr,logLevel)

        # Transform tbuild log file to readable format
        self._transformTbuildLogFile(stdout)

        if returncode == 0:
            # Check if there is error record
            returncode = self._checkErrorTables(stdout)
        return returncode
