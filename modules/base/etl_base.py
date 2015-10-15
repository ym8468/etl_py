from ConfigParser import SafeConfigParser
from subprocess import Popen, PIPE
from dateutil.relativedelta import relativedelta
import datetime
import fnmatch
import logging
import os.path
import os
import re
import sys
import shutil
import uuid
import codecs
import jpype
import csv
import posixfile

from modules.base.exceptions     import FileConfigMissing,MandatoryOptionMissing
from modules.base.constants      import *
from modules.base.environment    import Environment
from modules.base.options        import Options
import modules.base.etl_util    as etl_util

class EtlBase(object):
    def __init__(self):
        self.options = Options()
        self.config = SafeConfigParser()
        self.config.read(os.path.join(etl_util.get_app_root_path(), 'conf/globals.cfg'))
        env = Environment(self.config)
        self.environment   = env.environment
        self.environmentId = env.environmentId
        self._persistentParams = {}

    #####################
    # Private Functions #
    #####################
    def _createDateString(self, dateFormat, returnDays = None, returnMonths = None):
        currentTime = datetime.datetime.now()
        if (returnMonths != None):
            numWeeks        = 4 * int(returnMonths)                  # TODO: Improve this part to ensure it always returns correctly
            delta           = datetime.timedelta(weeks = numWeeks)
            requestedTime   = currentTime - delta
        elif (returnDays != None):
            delta           = datetime.timedelta(days = int(returnDays))
            requestedTime   = currentTime - delta
        else:
            requestedTime = currentTime

        dateString = None
        if (dateFormat == 'YYYYMMDDhhmiss'):
            dateString  = "%s%s%s" % (requestedTime.year   , str(requestedTime.month).rjust(2,'0') , str(requestedTime.day).rjust(2,'0'))
            dateString += "%s%s%s" % (str(requestedTime.hour).rjust(2,'0') , str(requestedTime.minute).rjust(2,'0') , str(requestedTime.second).rjust(2,'0'))
        elif (dateFormat == 'YYYYMMDD'):
            dateString = "%s%s%s" % (requestedTime.year   , str(requestedTime.month).rjust(2,'0') , str(requestedTime.day).rjust(2,'0'))
        elif (dateFormat == 'YYYY/MM/DD_hh:mi:ss'):
            dateString = "%s/%s/%s_" % (requestedTime.year   , str(requestedTime.month).rjust(2,'0') , str(requestedTime.day).rjust(2,'0'))
            dateString  = "%s:%s:%s" % (requestedTime.year   , str(requestedTime.month).rjust(2,'0') , str(requestedTime.day).rjust(2,'0'))
        elif (dateFormat == 'YYYY-MM-DD'):
            dateString = "%s-%s-%s" % (requestedTime.year   , str(requestedTime.month).rjust(2,'0') , str(requestedTime.day).rjust(2,'0'))
        elif (dateFormat == 'YYYYMM'):
            dateString = "%s%s" % (requestedTime.year   , str(requestedTime.month).rjust(2,'0'))
        elif (dateFormat == 'YYYY/MM'):
            dateString = "%s/%s" % (requestedTime.year   , str(requestedTime.month).rjust(2,'0'))
        else:
            msg = "The Date Format \"%s\" has not been implemented. Aborting Process" % (dateFormat)
            logging.error(msg)
            sys.exit(RC_ERROR)

        return dateString

    def _createLockFile(self, lockFilename=None):
        '''
        Creates a lock file with the specified name. If no name is provided the calling action name will be used
        '''
        basePath =  os.getcwd()
        if (lockFilename == None):
           lockFilename = basePath + '/' + LOCKFILE_DIRECTORY + '/' +  str(self.options.get('action')) + '.lock'
        print lockFilename
        lockFile = open(lockFilename,'w')
        return lockFilename

    def _createWorkDirectory(self, workDirectory):
        '''
        Creates a work directory
        '''
        if not os.path.exists(workDirectory):
           logging.warn("The work directory '%s' does not exist. Attempting to create it." % (workDirectory))
           try:
               os.makedirs(workDirectory)
               logging.warn("Work Directory '%s' has been created" % (workDirectory))
           except:
               raise IOError('not exist the work directory. (%s)' % workDirectory)

    def _removeWorkDirectory(self, workDirectory):
        '''
        Removes a work directory which has directories or files
        '''
        if os.path.exists(workDirectory):
           logging.warn("The work directory '%s' exists. Attempting to remove it." % (workDirectory))
           try:
               shutil.rmtree(workDirectory)
               logging.warn("Work Directory '%s' has been removed" % (workDirectory))
           except Exception, e:
               raise IOError('not remove the work directory. (%s)' % workDirectory)

    def _deleteLockFile(self, lockFilename=None):
        '''
        Delete a lock file with the specified name. If no name is provided the calling action name will be used
        '''
        basePath =  os.getcwd()
        if (lockFilename == None):
           lockFilename = basePath + '/' + LOCKFILE_DIRECTORY + '/' + str(self.options.get('action')) + '.lock'
        os.unlink(lockFilename)

    def _executeOsCommand(self, cmdStr, extraEnvVariables={}, logLevel=logging.DEBUG, waitTillFinished=True):
        '''
        Execute an os command
        '''
        cmdStrMsg = cmdStr
        # Remove any mysql passwords from logging
        if ('mysql' in cmdStr):
           passwordStart = cmdStr.find('-p')
           if (passwordStart > 0):
              passwordEnd = cmdStr.find(' ',passwordStart)
              cmdStrMsg = cmdStr[0:passwordStart] + cmdStr[passwordEnd:]
        msg = "Executing command [%s]" % (cmdStrMsg)
        logging.log(logLevel, msg)

        # Prepare the environment for the subprocess
        myEnv = os.environ
        for envVar in extraEnvVariables:
            myEnv[envVar] = extraEnvVariables[envVar]

        # Read the log directory location
        logDirectory = self.config.get(SECTION_DIRECTORIES, 'LOG_DIRECTORY')
        uid            = str(uuid.uuid4())
        stdinFilePath  = "%s/%s%s" % (logDirectory, STDIN_FILE, uid)
        stdinFile      = open(stdinFilePath  ,'w')
        uid            = str(uuid.uuid4())
        stdoutFilePath = "%s/%s%s" % (logDirectory, STDOUT_FILE, uid)
        stdoutFile     = open(stdoutFilePath ,'w')
        uid            = str(uuid.uuid4())
        stderrFilePath = "%s/%s%s" % (logDirectory, STDERR_FILE, uid)
        stderrFile     = open(stderrFilePath ,'w')
        p = Popen(cmdStr, shell=True, stdin=stdinFile, stdout=stdoutFile, stderr=stderrFile, close_fds=True, env=myEnv)

        # Return currently running process if waitTillFinished is False
        if (waitTillFinished == False):
           return (p, stdinFilePath, stdoutFilePath, stderrFilePath)

        # Wait till process is finished
        returnCode = p.wait()
        logging.log(logLevel, "Command Return Code is %s" % (returnCode))

        # Create stdin array
        stdinFile  = open(stdinFilePath  ,'r')
        stdin = []
        for line in stdinFile.readlines():
            stdin.append(line.strip())

        # Create stdin array
        stdoutFile = open(stdoutFilePath ,'r')
        stdout = []
        for line in stdoutFile.readlines():
            stdout.append(line.strip())

        # Create stdin array
        stderrFile = open(stderrFilePath ,'r')
        stderr = []
        for line in stderrFile.readlines():
            stderr.append(line.strip())

        # Delete files
        os.unlink(stdinFilePath)
        os.unlink(stdoutFilePath)
        os.unlink(stderrFilePath)

        return (stdin, stdout, stderr, p.returncode)

    def _isInteger(self, variable):
        try:
            int(variable)
            return True
        except ValueError:
            return False

    def _getDelimeter(self, delimString):
        delim = delimString
        # Determine the delim to use
        if (delimString == 'comma'):
             delim  = ","
        elif (delimString == 'tab'):
             delim  = "\t"
        elif (delimString == 'pipe'):
             delim  = '|'
        return delim

    def _getMatchingFiles(self, sourceDir, filePattern, recursiveFlg=False, backDays=0):
        if recursiveFlg:
            matchingFiles = self._getMatchingFilesRecursively(sourceDir, filePattern)
        else:
            (matchingFiles, matchingDirectories) = self._getMatchingFilesAndDirectories(sourceDir, filePattern)
        if not backDays:
            return matchingFiles
        returnFiles = []
        for file in matchingFiles:
            modifiedTime = datetime.datetime.fromtimestamp(os.path.getmtime(file))
            if datetime.datetime.now() - modifiedTime > datetime.timedelta(days=int(backDays)):
                returnFiles.append(file)
        return returnFiles

    def _getMatchingDirectories(self, sourceDir, filePattern):
        (matchingFiles,matchingDirectories) =self._getMatchingFilesAndDirectories(sourceDir, filePattern)
        return matchingDirectories

    def _getMatchingFilesAndDirectories(self, sourceDir, filePattern):
        filesToCheck        = sorted(os.listdir(sourceDir))
        matchingFiles       = []
        matchingDirectories = []
        for file in filesToCheck:
            for subPattern in filePattern.split(','):
                if re.match(subPattern, file) or fnmatch.fnmatch(file, subPattern):
                    fullPath = "%s/%s" % (sourceDir, file)
                    dirFlg = os.path.isdir(fullPath)
                    if dirFlg:
                        logging.debug("The directory '%s' matches the pattern '%s'." % (file, subPattern))
                        matchingDirectories.append(fullPath)
                    else:
                        logging.debug("The file '%s' matches the pattern '%s'." % (file, subPattern))
                        matchingFiles.append(fullPath)
        return (matchingFiles,matchingDirectories)

    def _getMatchingFilesRecursively(self, sourceDir, filePattern):
        allMatchingFiles = []
        for root, dirnames, filenames in os.walk(sourceDir):
            (matchingFiles, matchingDirectories) = self._getMatchingFilesAndDirectories(root, filePattern)
            allMatchingFiles += matchingFiles
        return allMatchingFiles

    def _getNewline(self, newlineString):
        # Determine newline
        if (newlineString == 'windows'):
            newline = "\r\n"
        elif (newlineString == 'unix'):
            newline = "\n"
        elif (newlineString    == 'nothing'):
            newline = ''
        return newline

    def _hasLockFile(self, lockFilename=None):
        '''
        Checks if a lock file exists. Returns True or False.
        '''
        basePath =  os.getcwd()
        if (lockFilename == None):
           lockFilename = basePath + '/' + LOCKFILE_DIRECTORY + '/' + str(self.options.get('action')) + '.lock'
        return os.path.exists(lockFilename)

    def _log(self, msg, stdout, stderr, logLevel = 'debug'):
        '''
        Prints lines from both stdout and stderr
        log level for stdout is specified via logLevel
        '''
        validLevels = ['error','warn','info','debug']
        if (logLevel not in validLevels):
            logging.error('Invalid Level')
            return
        if logLevel == 'error':
            myLog = logging.error
        elif logLevel == 'warn':
            myLog = logging.warn
        elif logLevel == 'info':
            myLog = logging.info
        else:
            myLog = logging.debug
        myLog(msg)
        if (len(stdout) > 0):
            myLog('--STANDARD OUTPUT--')
            for line in stdout:
                # Remove any mysql passwords from logging
                if ('.logon' in line):
                    passwordStart = line.find('/')
                    if (passwordStart > 0):
                       line = line[0:passwordStart]
                myLog(line.strip())
        if (len(stderr) > 0):
           logging.error('--STANDARD ERROR--')
           for line in stderr:
               logging.error(line.strip())

    def _matchValues(self, value, valueList):
        if not valueList:
            return True
        else:
            return value in valueList

    def _matchRange(self, value, (min, max)):
        if min is not None and (value is None or  value < min):
            return False
        if max is not None and (value is None or  value > max):
            return False
        return True

    def _matchMonthDay(self, timestamp, (fromMonth, fromDay, toMonth, toDay)):
        if not fromMonth or not fromDay or not toMonth or not toDay:
            return True
        if timestamp is None:
            return False
        if (fromMonth, fromDay) <= (toMonth, toDay):
            return (fromMonth, fromDay) <= (timestamp.month, timestamp.day) <= (toMonth, toDay)
        else:
            return (fromMonth, fromDay) <= (timestamp.month, timestamp.day) or (timestamp.month, timestamp.day) <= (toMonth, toDay)

    def _substituteVariables(self, bteqFilename):
        '''
        This function takes a given filename, performs the standard substitutions for the file
        '''
        irregularEnvVariables = ["LOGON_USER","LOAD_FILE","DWH_DBNAME","DWH_JOBDATE","DWH_JOBYYYYMM","DWH_BR","DWH_BRNO","USER_LOGON","DWH_JOBCODE","TARGET_DB"]
        regularEnvVariables   = {'DB_INPUT':(1,21),'BATCH_PARAM':(1,201),'DB_OUTPUT':(1,21),'FILE_INPUT':(1,21),'FILE_OUTPUT':(1,21),'SUB_JOB_DATE':(1,11),'SUB_UPD_DATE':(1,11)}
        substitutions         = {}

        # Create the irregular Environment Variable Subtitutions
        for envVar in irregularEnvVariables:
            if (os.environ.has_key(envVar) == True):
                envVarVal = os.environ[envVar]
            else:
                envVarVal = ''
            substitutions[envVar] = envVarVal

        # Create the regular Environment Variable Substitutions
        for (prefix, prefixRange) in regularEnvVariables.iteritems():
            for i in range(prefixRange[0],prefixRange[1]):
                envVar = "%s_%s" % (prefix, str(i).rjust(2,'0'))
                print envVar

                if (os.environ.has_key(envVar) == True):
                    envVarValue = os.environ[envVar]
                else:
                    envVarValue = ''
                substitutions[envVar] = envVarValue

        # Open File and Perform substitutions
        bteqFile = open(bteqFilename, 'r')
        output   = ""
        for line in bteqFile.readlines():
            newLine = line
            for (key, value) in substitutions.iteritems():
                newLine = newLine.replace(key,value)
            output += newLine
        bteqFile.close()

        return output

    def _hasOption(self, mOpt):
        if (self.options.hasKey(mOpt) == False):
           return False
        if (self.options.get(mOpt) == None):
           return False
        return True

    def _validateOptions(self, mandatoryOptions, mandatoryValues):
        for optGroup in mandatoryOptions:
            optionFound = False
            for optGrpOpt in optGroup.split('|'):
                if self._hasOption(optGrpOpt):
                   optionFound = True
                   break
            mOpt = optGrpOpt
            if (optionFound == False and optGroup.find('|') > 0):
               print "One of the following options needs to be supplied before executing this action '%s'" % (optGroup)
               sys.exit(1)
            elif (optionFound == False):
               print "The mandatory option \"%s\" has not been assigned a value."  %  (mOpt)
               sys.exit(1)
            # Check if there are any white listed values for this given
            # option
            if mandatoryValues.has_key(mOpt):
                legalOptValues = mandatoryValues[mOpt]
                if (self.options.get(mOpt) not in legalOptValues):
                    print "The option \"%s\" has been assigned an illegal value. Choose from the list below" % (mOpt)
                    print "--------------------------------------------------------------------------------"
                    legalOptValues.sort()
                    for legalOptValue in legalOptValues:
                        print legalOptValue
                    sys.exit(1)

    def _readFileConfiguration(self, file, sectionName = SECTION_DEFAULT):
        # TODO: get the base path from conf/globals.cfg [directory] section
        # remove section info of previous one
        if self.config.remove_section(sectionName):
            logging.info("The [%s] info of previous one is removed." % (sectionName))
        fileConfigPath = "conf/files/%s.cfg" % (file)
        if (os.path.exists(fileConfigPath) == False):
           msg = "The configuration file '%s' could not be found. Please recreate and try again" % (fileConfigPath)
           logging.error(msg)
           raise FileConfigMissing(msg)
        msg = "Reading configuration file '%s' " % (fileConfigPath)
        logging.info(msg)
        self.config.read(fileConfigPath)

    def _readFilesToBeProcessed(self):
        '''
        Creates a list of files that need to be processed
        '''
        filesToBeProcessed = []
        if (self.options.get('files') != None):
            confBaseDir = "conf/files/%s" % (self.options.get('files'))
            for filename in os.listdir(confBaseDir):
                if (filename.endswith('.cfg') == True):
                    fullFilename = "%s/%s" % (self.options.get('files'), filename.replace('.cfg',''))
                    filesToBeProcessed.append(fullFilename)
        else:
           filesToBeProcessed = [self.options.get('file')]

        return filesToBeProcessed

    def _readTableConfiguration(self, table, sectionName = SECTION_DEFAULT):
        # TODO: get the base path from conf/globals.cfg [directory] section
        # remove section info of previous one
        if self.config.remove_section(sectionName):
            logging.info("The [%s] info of previous one is removed." % (sectionName))
        tableConfigPath = "conf/data/%s.cfg" % (table)
        if (os.path.exists(tableConfigPath) == False):
           msg = "The configuration file '%s' could not be found. Please recreate and try again" % (tableConfigPath)
           logging.error(msg)
           raise FileConfigMissing(msg)
        msg = "Reading configuration file '%s' " % (tableConfigPath)
        logging.info(msg)
        self.config.read(tableConfigPath)

    def readOptionFromCmd(self, keyName, defaultValue):
        if self.options.hasKey(keyName) and self.options.get(keyName) != None:
            optionValue = self.options.get(keyName)
            logging.debug("The option '%s' is set to '%s', via command line argument" % (keyName, optionValue))
        else:
            optionValue = defaultValue
            logging.debug("The option '%s' is set to '%s', via the default value" % (keyName, optionValue))
        try:
            optionValueConverted = int(optionValue)
        except:
            optionValueConverted = optionValue
        return optionValueConverted

    def readMandatoryOptionFromCmd(self, keyName):
        optionValue = self.readOptionFromCmd(keyName, None)
        if optionValue is None:
            msg = "The mandatory option '%s' is missing from command line." % (keyName)
            raise MandatoryOptionMissing(msg)
        return optionValue

    def _readOption(self, section, option, defaultValue, logMessage = True, canOverride = True):
        # 1. Check if the option was supplied on the command line
        # 2. Check if exists in the config file
        # 3. set it to the default
        #if hasattr(self.options, option) and getattr(self.options, option) != None and canOverride:
        section = self._getActualSectionName(section)
        if self.options.hasKey(option) and self.options.get(option) != None and canOverride:
           optionValue = self.options.get(option)
           msg         = "The option '%s' set to '%s', via command line argument" % (option, optionValue)
        elif (self.config.has_option(section, option) == False or not self.config.get(section, option)):
           optionValue = defaultValue
           msg         = "The option '%s' is set to the default value of '%s'" % (option, optionValue)
        else:
           optionValue = self.config.get(section, option)
           msg = "The option '%s' is set to the value of '%s', via configuration file section [%s]" % (option, optionValue, section)
        # Log what happened if requested
        if (logMessage):
           logging.debug(msg)
        #convert the string read from the cfg file into a int
        try:
            optionValueConverted = int(optionValue)
        except:
            optionValueConverted = optionValue
        return optionValueConverted

    def _readMandatoryOption(self, section, option, logMessage = True):
        section = self._getActualSectionName(section)
        if (self.config.has_option(section, option) == False):
           msg = "The mandatory option '%s' is missing from the section [%s]" % (option, section)
           raise MandatoryOptionMissing(msg)
        else:
           optionValue = self.config.get(section, option)
           if (logMessage):
               msg = "The option '%s' in [%s] is set to '%s'" % (option, section, optionValue)
               logging.debug(msg)
        #convert the string read from the cfg file into a int
        try:
            optionValueConverted = int(optionValue)
        except:
            optionValueConverted = optionValue
        return optionValueConverted

    def _readOptionDefault(self, section, option, defaultValue):
        section = self._getActualSectionName(section)
        if self.config.has_option(section, option):
            optionValue = self._readOption(section, option, defaultValue)
        else:
            optionValue = self._readOption(SECTION_DEFAULT, option, defaultValue)
        return optionValue

    def _readMandatoryOptionDefault(self, section, option):
        optionValue = self._readOptionDefault(section, option, None)
        if optionValue is None:
            msg = "The mandatory option '%s' is missing from the section [%s]" % (option, section)
            raise MandatoryOptionMissing(msg)
        return optionValue

    def _readDirOption(self, section, option, defaultValue, checkDefault=True):
        if checkDefault:
            dirLookup = self._readOptionDefault(section, option, defaultValue)
        else:
            dirLookup = self._readOption(section, option, defaultValue)
        if not dirLookup:
            return dirLookup
        return self._readMandatoryOption(SECTION_DIRECTORIES, dirLookup)

    def _readDirMandatoryOption(self, section, option, checkDefault=True):
        res = self._readDirOption(section, option, None, checkDefault)
        if res is None:
            msg = "The mandatory option '%s' is missing from the section [%s]" % (option, section)
            raise Exception(msg)
        return res

    def _readPatternOption(self, section, option, defaultValue, checkDefault=True):
        if checkDefault:
            patternString = self._readOptionDefault(section, option, defaultValue)
        else:
            patternString = self._readOption(section, option, defaultValue)
        return self.substitutePatternString(patternString)

    def _readPatternMandatoryOption(self, section, option, checkDefault=True):
        res = self._readPatternOption(section, option, None, checkDefault)
        if res is None:
            msg = "The mandatory option '%s' is missing from the section [%s]" % (option, section)
            raise Exception(msg)
        return res

    def _readBooleanOption(self, section, option, defaultValue, checkDefault=True):
        if checkDefault:
            booleanString = self._readOptionDefault(section, option, defaultValue)
        else:
            booleanString = self._readOption(section, option, defaultValue)
        if str(booleanString).strip().lower() == 'true':
            return True
        if str(booleanString).strip().lower() == 'false':
            return False
        return defaultValue

    def _readBooleanMandatoryOption(self, section, option, checkDefault=True):
        res = self._readBooleanOption(section, option, None, checkDefault)
        if res is None:
            msg = "The mandatory option '%s' is missing from the section [%s]" % (option, section)
            raise Exception(msg)
        return res

    def _equalList(self, list1, list2):
        """
        Checks if two lists are completely same.
        """
        flg1=self._includeList(list1, list2)
        flg2=self._includeList(list2, list1)
        return flg1 and flg2

    def _includeList(self, list1, list2):
        """
        Checks if two lists are partially same.
        In fact, return True when all elements in list1 are in list2 from the beginning
        e.g.,
        list1=['a','b']
        list2=['a','b','c','d']
        """
        flg=True
        i=0
        for data in list1:
            try:
                tmp=list2[i]
            except Exception, msg:
                tmp=None
            if not data==tmp :
                flg=False
            i=i+1
        return flg

    def _differList(self, list1, list2):
        """
        Gets the following elements
        -elements of list1 which are not in list2
        -elements of list2 which are not in list1
        """
        #Element in list1 is not in the same position of list2
        rtn1=[]
        #Element in list1 is not in list2
        rtn2=[]
        #Element in list2 is not in list1
        rtn3=[]
        errflg=False
        i=0
        for data1 in list1:
            try:
                data2=list2[i]
            except Exception, msg:
                data2=None
            if not data1==data2 :
                errflg=True
                if data1 in list2 :
                    rtn1.append(data1)
                else:
                    rtn2.append(data1)
            i=i+1
        i=0
        for data2 in list2:
            if not data2 in list1 :
                rtn3.append(data2)
                errflg=True
            i=i+1
        if errflg:
            return (rtn1,rtn2,rtn3)
        else:
            return None

    def _getElements(self, row, nums):
        """
        Gets elements which are specified by nums
        """
        i=0
        key=[]
        for val in row:
            if i in nums:
                key.append(val)
            i=i+1
        return key

    def _addDateStr(self,strdate,adddates,fmt):
        """
        Adds date with some number
        """
        strdata2=strdate
        try:
            date1 = datetime.datetime.strptime(strdate, fmt)
            date2 =  date1 + datetime.timedelta(days=adddates)
            strdata2 = date2.strftime(fmt)
        except Exception, msg:
            err=''
            functionName       = sys._getframe(0).f_code.co_name
            err=err+'[Failed on %s]%s %s' % (functionName,strdata2,str(msg))
            logging.error(err)
        return strdata2

    def _chgDateFomat(self,strdate,fmt1,fmt2,datetimeFlg=0):
        """
        Changes date format
        """
        try:
            if datetimeFlg==1:
                tmp=strdate.split(" ")
                strdate=tmp[0]
            date1 = datetime.datetime.strptime(strdate, fmt1)
            strdata2 = date1.strftime(fmt2)
            if datetimeFlg==1:
                strdata2 += (' ' +tmp[1])
            return strdata2
        except Exception, e:
            logging.error(str(e))
            raise

    def _getTargetDateList(self,target_date, days_back=0):
        """
        TODO:duplicate with mysql.py!!!   should remove this function from mysql.py
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
            date          = datetime.datetime.strptime(target_date, '%Y%m%d')
            calc_date     = date + datetime.timedelta(days=-back_day)
            str_calc_date = calc_date.strftime('%Y-%m-%d')
            date_list.append(str_calc_date)
        return date_list

    def _getDelimeterByType(self, fileType):
        """
        Determines the delimeter
        """
        if (fileType == 'csv'):
             delim  = ","
        elif (fileType == 'tsv'):
             delim  = "\t"
        elif (fileType == 'pipe'):
             delim  = '|'
        return delim

    def _getActualSectionName(self, origSection):
        section = "%s_%s" % (self.environment, origSection)
        if not self.config.has_section(section):
            section = origSection
        return section

    def _getActualTeradataSchema(self, dbName):
        """
        Get Check actual database name
        Args:
            dbName: logical database name
        Returns:
            actual_db: actual database name
        """
        actual_db=self.environmentId+dbName
        return actual_db

    def _getSchemaName(self, databaseKey):
        """
        Get schema names
        e.g.,
        Args:
            databaseKey    : sample1_item(or sample_item)
        Returns:
            schema_teradata : sample
            schema_mysql    : item
        e.g.,
        Args:
            databaseKey    : sample1_gpoint_es
        Returns:
            schema_teradata : sample
            schema_mysql    : gpoint_es
        e.g.,
        Args:
            databaseKey    : review
        Returns:
            schema_teradata : review
            schema_mysql    : review
        """
        if 'sample1' in databaseKey:
            schema_teradata = 'sample'
            schema_mysql    = databaseKey.replace('sample1_','')
        elif 'sample' in databaseKey:
            schema_teradata = 'sample'
            schema_mysql    = databaseKey.replace('sample_','')
        else:
            schema_teradata = databaseKey
            schema_mysql    = databaseKey
        return (schema_teradata,schema_mysql)

    def _translateDBValue(self, ele):
        """
        Translate DB values which are gotten by jdbc to Python values(utf-8,int,float,long)
        e.g.,
        DB values    :jpype.java.lang.Integer
        Python values:long
        """
        if ele is None:
            ele2 = None
        elif isinstance(ele, unicode):
            # DBType:String
            # removes spaces on right side because teradata string is 3 times as big as real size.
            # removes spaces not only for teradata but also for other db because of value check.
            # if ele.rstrip(), remove both 0020(space i.e., " ") and 00A0(nbsp)
            ele2 = ele.rstrip(' ')
            # unicode  --> utf-8'
            ele2 = ele2.encode('utf-8')
        elif isinstance(ele, str):
            # DBType:MySQL#TIMESTAMP
            # Thanks to metatable#getColumns, DATETIME or DATE will be gotten as unicode string
            # DBType:Teradata#TIMESTAMP,DATE
            ele2 = ele
        elif isinstance(ele, int):
            # DBType:SMALLINT,INT
            ele2 = ele
        elif isinstance(ele, float):
            # DBType:DOUBLE,DECIMAL
            ele2 = ele
        elif isinstance(ele, jpype.java.lang.Integer):
            # DBType:TINYINT
            ele2 = long(ele.toString())
        elif isinstance(ele, jpype.java.math.BigInteger):
            # DBType:BIGINT 1 etc
            ele2 = long(ele.toString())
        elif isinstance(ele, jpype.java.lang.Long):
            # DBType:BIGINT max value
            ele2 = long(ele.toString())
        else:
            # peculiar object
            ele2 = (ele.toString()).encode('utf-8')
        return ele2

    def _readFile(self, filePath, characterSet, delim=None):
        """
        Reads flatfiles with csv.reader
        Args:
            characterSet:e.g., utf-8,sjis
            delim       :e.g., '\t'

        Return:two dimensional table
            e.g.,
            Input Flatfile
                a,b \n
                c,d \n

            If delim=','
                [['a','b'],['c','d']]

            If delim=None
                ['a,b','c,d']

        """
        rowList=[]
        f = codecs.open(filePath,"r",characterSet)
        for rowstr in f:
            #For each line, the last letter should be removed
            rowstr = rowstr.rstrip("\n")
            if delim is None:
                row=rowstr
            else:
                row=rowstr.split(delim)
            rowList.append(row)
        f.close()
        return rowList

    def _writeFile(self, filename, val, characterSet, addflg):
        """
        Writes string into Flatfile

        Args:
            addflg:
                w:new
                a:add
        """
        try:
            # flash val to flatfile
            #Regardless of characterSet, val should be unicode
            #val=val.decode('utf-8')
            f = codecs.open(filename, addflg, characterSet)
            f.write(val)
            f.close()
            return 0
        except Exception, e:
            functionName       = sys._getframe(0).f_code.co_name
            err='[Failed on %s]%s' % (functionName,filename)
            logging.error(err)
            return 1

    def _deleteFile(self, filename):
        """
        Deletes a file

        Args: filename to delete
        """
        try:
            os.remove(filename)
            return 0
        except Exception, e:
            functionName       = sys._getframe(0).f_code.co_name
            err='[Failed on %s]%s' % (functionName,filename)
            logging.error(err)
            return 1

    def _deletePersistentParamsFile(self):
        '''
        Removes the persistent parameter file
        '''
        jobname = os.getenv('ML_JOBNET_NAME') + os.getenv('THREAD', '')

        if jobname is not None:
            paramsDirectoryBasePath = self.config.get(SECTION_DIRECTORIES, PARAMETERS_DIRECTORY)
            paramsFilePath = os.path.join(paramsDirectoryBasePath, jobname.lower()+".prm")
            try:
                if os.path.isfile(paramsFilePath):
                    self._deleteFile(paramsFilePath)
                    logging.warn("Persistent params file '%s' has been deleted" % (paramsFilePath))
                else:
                    logging.warn("Persistent params file '%s' did not exist" % (paramsFilePath))
            except Exception, e:
                logging.warn("Persistent params file '%s' has not been deleted: %s" % (paramsFilePath, e))
                return 1
        else:
            return 0

    def _createPersistentParamsDirectory(self):
        '''
        Creates the parameters directory
        '''
        paramsDirectoryBasePath = self.config.get(SECTION_DIRECTORIES, PARAMETERS_DIRECTORY)
        if not os.path.exists(paramsDirectoryBasePath):
           logging.warn("The parameters directory '%s' does not exist. Attempting to create it." % (paramsDirectoryBasePath))
           try:
               os.makedirs(paramsDirectoryBasePath)
               logging.warn("Params directory '%s' has been created" % (paramsDirectoryBasePath))
           except:
               logging.warn('not exist the parameters directory. (%s)' % paramsDirectoryBasePath)
               return 1
        else:
            self._deletePersistentParamsFile()

        return 0

    def _loadPersistentParameters(self):
        '''
        Reads from the parameter file, according to the ML_JOBNET_NAME environment parameter
        The OPTIONAL environment flag 'THREAD', if set, allows to distinguish several processes that run concurrently in the same jobnet
        '''
        jobname = os.getenv('ML_JOBNET_NAME') + os.getenv('THREAD', '')

        if jobname is not None:
            paramsDirectoryBasePath = self.config.get(SECTION_DIRECTORIES, PARAMETERS_DIRECTORY)
            paramsFilePath = os.path.join(paramsDirectoryBasePath, jobname.lower()+".prm")
            readParams = {}
            try:
                if os.path.isfile(paramsFilePath):
                    with open(paramsFilePath, 'rb') as f:
                        #f.lock('r|')
                        reader = csv.reader(f, delimiter=',')
                        for line in reader:

                            # Convert strings back to numbers
                            try:
                                line[0] = int(line[0])
                            except ValueError:
                                pass
                            try:
                                line[1] = int(line[1])
                            except ValueError:
                                pass
                            readParams[line[0]] = line[1]
                        #f.lock('u')
                        f.close()
            except Exception, e:
                logging.info("Error reading from %s: %s" % (paramsFilePath, e))
                return {}
            self._persistentParams = readParams
        else:
            logging.info("The environment variable '%s' is not set." % 'ML_JOBNET_NAME')
            self._persistentParams = {}


    def _savePersistentParameters(self):
        '''
        Writes to the parameter file, according to the ML_JOBNET_NAME environment parameter
        The OPTIONAL environment flag 'THREAD', if set, allows to distinguish several processes that run concurrently in the same jobnet
        '''
        jobname = os.getenv('ML_JOBNET_NAME') + os.getenv('THREAD', '')

        if jobname is not None:
            self._createPersistentParamsDirectory()
            paramsDirectoryBasePath = self.config.get(SECTION_DIRECTORIES, PARAMETERS_DIRECTORY)
            paramsFilePath = os.path.join(paramsDirectoryBasePath, jobname.lower()+".prm")
            paramsFile = posixfile.open(paramsFilePath, 'a')
            try:
                paramsFile.lock('w|')
            except IOError as e:
                logging.warning("Warning! Locking not possible: %s" % e)
            writer = csv.writer(paramsFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for p in self._persistentParams:
                try:
                    #paramsFile.write("\"%s\",\"%s\"\n" % (p, self._persistentParams[p]))
                    writer.writerow([p, self._persistentParams[p]])
                except Exception, e:
                    logging.error("Error writing to %s: %s" % (paramsFilePath, e))
                    paramsFile.lock('u')
                    return 1
            paramsFile.lock('u')
            paramsFile.close()
            return 0
        else:
            logging.info("The environment variable '%s' is not set." % 'ML_JOBNET_NAME')
            return 0

    def _putPersistentParameters(self, parameters):
        '''
        Writes to the 'self' parameter structure

        Args: dictionary of parameters to add
        '''
        for p in parameters:
            self._persistentParams[p] = parameters[p]

    def _getPersistentParameter(self, parameters):
        '''
        Reads from the 'self' parameter structure

        Args: list of parameters to get

        Returns: a list with the parameters, in the same order as requested
            If a parameter does not exist, it will return None in its place
        '''
        returnParams = None
        if type(parameters) is tuple:
            returnParams = []
            for e in parameters:
                try:
                    returnParams.append(self._persistentParams[e])
                except KeyError:
                    returnParams.append(None)
        elif type(parameters) is str or type(parameters) is int:
            try:
                returnParams = self._persistentParams[parameters]
            except KeyError:
                returnParams = None
        return returnParams

    def getDDLFile(self, dbtype, database, table):
        '''
        Read DDL file conf/ddl/dbtype/database/table.sql
        '''
        ddlFile = os.path.join('conf', 'ddl', dbtype, database, table+'.sql')
        if not os.path.exists(ddlFile):
            msg = "The DDL file '%s' could not be found!" % ddlFile
            raise Exception(msg)
        logging.debug("DDL file is '%s'" % (ddlFile))
        return ddlFile

    def getConfigFile(self, folderName, fileName):
        '''
        Read configure file conf/folderName/fileName.cfg
        '''
        configFile = os.path.join('conf', folderName, fileName+'.cfg')
        if not os.path.exists(configFile):
            msg = "The configuration file '%s' could not be found!" % configFile
            logging.error(msg)
            raise FileConfigMissing(msg)
        logging.debug("Configuration file is '%s'" % (configFile))
        return configFile

    def getActionConfigFile(self, fileName, actionName=''):
        '''
        Read configure file conf/actions/actionName/fileName.cfg
        '''
        folderName = os.path.join('actions', actionName)
        return self.getConfigFile(folderName, fileName)

    def getModuleConfigFile(self, fileName, folderName=''):
        '''
        Read configure file conf/modules/folderName/fileName.cfg
        '''
        folderName = os.path.join('modules', folderName)
        return self.getConfigFile(folderName, fileName)

    def getSectionsToBeProcessed(self, configFile, sectionPattern):
        '''
        Get all section names in configFile matched the sectionPattern
        '''
        self.config.read(configFile)
        allSections = self.config.sections()
        matchedSections = []
        for section in allSections:
            if fnmatch.fnmatch(section, sectionPattern):
                matchedSections.append(section)
        logging.info("Matched sections: %s" % (matchedSections))
        return matchedSections

    def getSectionsForAction(self, actionName, cfgOption='cfg', sectionOption='section'):
        '''
        Get all matched section names from cfgOption and sectionOption parameters in command line
        '''
        self._validateOptions([cfgOption,sectionOption],{})
        configFilename = self.options.get(cfgOption)
        sectionPattern = self.options.get(sectionOption)
        configFile = self.getActionConfigFile(configFilename, actionName)
        return self.getSectionsToBeProcessed(configFile, sectionPattern)

    def substitutePatternString(self, origStr):
        '''
        Substitute patterns in origStr
        '''
        if not origStr:
            return origStr
        logging.debug("origStr: %s" % (origStr))
        today     = datetime.datetime.today()
        todayStr    = "%s%s%s" % (today.year, str(today.month).rjust(2,'0'), str(today.day).rjust(2,'0'))
        oneDay    = datetime.timedelta(days=1)
        yesterday = today - oneDay
        yesterdayStr = "%s%s%s" % (yesterday.year, str(yesterday.month).rjust(2,'0'), str(yesterday.day).rjust(2,'0'))
        rndString = str(uuid.uuid4())
        thisMonthStr = today.strftime(PYTHON_DATE_FORMAT['YYYYMM'])
        firstDay = datetime.date(day=1, month=today.month, year=today.year)
        lastDayOfLastMonth = firstDay - oneDay
        lastMonthStr = lastDayOfLastMonth.strftime(PYTHON_DATE_FORMAT['YYYYMM'])
        dayOfNextMonth = today + relativedelta(months=1)
        nextMonthStr = dayOfNextMonth.strftime(PYTHON_DATE_FORMAT['YYYYMM'])
        newStr = origStr.replace('<TODAY>', todayStr).replace('<YESTERDAY>', yesterdayStr).replace('<RANDOM>', rndString).replace('<NEXT_MONTH>', nextMonthStr).replace('<THIS_MONTH>', thisMonthStr).replace('<LAST_MONTH>', lastMonthStr)
        logging.debug("newStr: %s" % (newStr))
        return newStr

    def transformDateString(self, dateStr, inputFormat, outputFormat, daysBack=0):
        '''
        Transform date string from inputFormat to outputFormat
        '''
        logging.debug("inDateStr: %s" % (dateStr))
        if dateStr == TODAY:
            targetDate = datetime.datetime.today()
        elif dateStr == YESTERDAY:
            targetDate = datetime.datetime.today() - datetime.timedelta(days=1)
        else:
            try:
                targetDate = datetime.datetime.strptime(dateStr, PYTHON_DATE_FORMAT[inputFormat])
            except ValueError:
                msg = "Parse date string: '%s' faild as '%s' format!" % (dateStr, inputFormat)
                raise Exception(msg)
        outDateStr = (targetDate - datetime.timedelta(int(daysBack))).strftime(PYTHON_DATE_FORMAT[outputFormat])
        logging.debug("outDateStr: %s" % (outDateStr))
        return outDateStr

    def getDictionaryFromString(self, string, cleanFlg=False, delimiter=','):
        if not string:
            logging.debug("Got empty dictionary.")
            return {}
        dictionary = {}
        for field in string.split(delimiter):
            pair = field.split(':')
            if len(pair) < 2:
                msg = ("Error when parsing dictionary string: '%s'!" % field)
                raise Exception(msg)
            key = ':'.join(pair[:-1]).strip() if cleanFlg else ':'.join(pair[:-1])
            dictionary[key] = pair[-1].strip() if cleanFlg else pair[-1]
        logging.debug("Got dictionary: %s" % dictionary)
        return dictionary

    def getListFromString(self, string, cleanFlg=False, delimiter=','):
        if not string:
            logging.debug("Got empty list.")
            return []
        resList = []
        for field in string.split(delimiter):
            resList.append(field.strip() if cleanFlg else field)
        logging.debug("Got list: %s" % resList)
        return resList

    def getListFromFile(self, file, cleanFlg=True):
        if not os.path.exists(file):
            msg = "The file '%s' could not be found!" % file
            raise Exception(msg)
        resList = []
        with open(file, 'r') as f:
            for line in f.readlines():
                resList.append(line.strip() if cleanFlg else line)
        logging.debug("Got list: %s" % resList)
        return resList

    def putListToFile(self, list, file, cleanFlg=True, skipIfEmpty=False):
        logging.debug("Write list: %s to file: %s" % (list, file))
        if skipIfEmpty and not list:
            logging.info("Skipped creating file: %s" % file)
            return RC_NO_ERROR
        with open(file, 'w') as f:
            if cleanFlg:
                f.writelines([str(i).strip() + '\n' for i in list])
            else:
                f.writelines([str(i) + '\n' for i in list])
        return RC_NO_ERROR

    def getFileFromGlobalCfg(self, keyName):
        file = self._readMandatoryOption(SECTION_FILES, keyName)
        logging.debug("Got file path '%s' from global.cfg:" % file)
        return file

    def createFileBodyFromTemplate(self, template, parameters):
        # Get template file path
        templateFile = self.getFileFromGlobalCfg(template)
        logging.info("Using template file: '%s'" % templateFile)
        # Generate file body
        try:
            fileBody = open(templateFile,'r').read() % parameters
        except:
            logging.error("Error happened when trying to creating file body!")
            raise
        return fileBody

    def createFileFromTemplate(self, template, parameters, outputFile):
        try:
            fileBody = self.createFileBodyFromTemplate(template, parameters)
            with open(outputFile, 'w') as f:
                f.write(fileBody)
        except:
            msg("Error happened when trying to creating file '%s'!" % outputFile)
            raise Exception(msg)
        logging.info("The file '%s' was created." % outputFile)
        return RC_NO_ERROR

    def isValidDate(self, dateStr, inputFormat):
        try:
            logging.debug("Try to parse date string: '%s' in format '%s'" % (dateStr, inputFormat))
            datetime.datetime.strptime(dateStr, PYTHON_DATE_FORMAT[inputFormat])
            return True
        except ValueError:
            logging.debug("Parsing date string '%s' failed in format '%s'" % (dateStr, inputFormat))
            return False

    def getDatetimeFromString(self, dateStr, inputFormat, defaultValue=None):
        try:
            logging.debug("Try to parse date string: '%s' in format '%s'" % (dateStr, inputFormat))
            res = datetime.datetime.strptime(dateStr, PYTHON_DATE_FORMAT[inputFormat])
            return res
        except ValueError:
            logging.warning("Parsing date string '%s' failed in format '%s'" % (dateStr, inputFormat))
            return defaultValue

    def getLogDir(self):
        jobnetName = ''
        if os.environ.has_key('ML_JOBNET_NAME'):
            jobnetName = os.environ['ML_JOBNET_NAME']
        baseLogDir = self._readMandatoryOption(SECTION_DIRECTORIES, 'LOG_DIRECTORY')
        return os.path.join(baseLogDir, jobnetName)

    def updatePersistentParametersFile(self, parameters):
        self._loadPersistentParameters()
        self.recreatePersistentParametersFile(parameters)

    def recreatePersistentParametersFile(self, parameters):
        self._putPersistentParameters(parameters)
        self._savePersistentParameters()
