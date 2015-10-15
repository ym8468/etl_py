# Standard Python Modules
import logging

# ETL Modules
from modules.base.etl_base           import EtlBase
from modules.oracle.mysql             import MySQL
from modules.teradata.db              import TeradataDB
from modules.base.constants           import *

class ExtractActions(EtlBase):
    ###############
    # Initializer #
    ###############
    def __init__(self):
        super(ExtractActions, self).__init__()
        self.teradata          = TeradataDB()
        self.mysql             = MySQL()
        self.extractDB         = None
        self.extractors        = { MYSQL:self.mysql }

    #####################
    # Private Functions #
    #####################
    def _readExtractParameters(self, sectionName):
        '''
        Read parameters for extracting
        '''
        parameters = {}
        parameters[TDPID] = 'red'
        parameters[EXTRACTOR] = self._readMandatoryOptionDefault(sectionName, EXTRACTOR)
        parameters[DATABASE] = self._readMandatoryOptionDefault(sectionName, DATABASE)
        parameters[TABLE] = self._readMandatoryOptionDefault(sectionName, TABLE)
        parameters[TARGET_DIRECTORY] = self._readDirMandatoryOption(sectionName, TARGET_DIRECTORY)
        defaultOutputfilename = "%s_%s_%s.txt" % (parameters[DATABASE], parameters[TABLE], self._createDateString('YYYYMMDD'))
        parameters[OUTPUT_FILENAME] = self._readPatternOption(sectionName, OUTPUT_FILENAME, defaultOutputfilename)
        parameters[CHARACTER_SET] = self._readOptionDefault(sectionName, CHARACTER_SET, 'utf8')
        parameters[DELIMITER] = self._readOptionDefault(sectionName, DELIMITER, DEFAULT_DELIMETER)
        parameters[COLUMNS] = self._readOptionDefault(sectionName, COLUMNS, None)
        parameters[TARGET_DATE_COLUMN] = self._readOptionDefault(sectionName, TARGET_DATE_COLUMN, None)
        targetDateStr = self._readOptionDefault(sectionName, TARGET_DATE, TODAY)
        parameters[END_DATE] = self.transformDateString(targetDateStr, 'YYYYMMDD', 'YYYY-MM-DD')
        daysBack = self._readOptionDefault(sectionName, DAYS_BACK, 0)
        parameters[START_DATE] = self.transformDateString(targetDateStr, 'YYYYMMDD', 'YYYY-MM-DD', daysBack)
        parameters[MODE] = self._readOptionDefault(sectionName, MODE, DEFAULT_MODE)
        if parameters[MODE] == MODE_FULL:
            parameters[TARGET_DATE_COLUMN] = None
        for key in parameters:
            logging.info("parameters['%s']:%s" % (key, parameters[key]))
        return parameters

    ####################
    # Public Functions #
    ####################
    def simpleExtract(self):
        '''
        USAGE: etl.py --action=simpleExtract --cfg=<config> --section=<section_pattern>
        It will read the options of each section matched <section_pattern> in conf/actions/simpleExtract/<config>.cfg
        EXAMPLE: python2.7 etl.py --action=simpleExtract --cfg=sample/newsletter --section=newsletter*
        '''
        self._validateOptions(['cfg','section'],{})
        configFilename = self.options.get('cfg')
        sectionPattern = self.options.get('section')
        configFile = self.getActionConfigFile(configFilename, SIMPLE_EXTRACT_ACTION)
        sections = self.getSectionsToBeProcessed(configFile, sectionPattern)
        errSections = []
        for section in sections:
            logging.info("\nStart processing section: %s" % section)
            parameters = self._readExtractParameters(section)
            self.extractDB = self.extractors[parameters[EXTRACTOR]]
            returnCode = self.extractDB.extractToFile(parameters)
            if returnCode != RC_NO_ERROR:
                errSections.append(section)
        if not errSections:
            logging.info("\nThe '%s' action finished successfully." % SIMPLE_EXTRACT_ACTION)
            return RC_NO_ERROR
        for errSection in errSections:
            logging.error("Error happened when processing section: '%s'" % errSection)
        return RC_ERROR
