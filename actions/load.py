# Standard Python Modules
import logging
import os.path

# Super DB Modules
from modules.base.etl_base           import EtlBase
from modules.oracle.mysql             import MySQL
from modules.teradata.db              import TeradataDB
from modules.teradata.fastload        import Fastload
from modules.teradata.tpt             import TPT
from modules.base.constants           import *

class LoadActions(EtlBase):
    ###############
    # Initializer #
    ###############
    def __init__(self):
        super(LoadActions, self).__init__()
        self.fastload          = Fastload()
        self.tpt               = TPT()
        self.teradata          = TeradataDB()
        self.mysql             = MySQL()

        self.loader = None
        self.targetDB = None

        self.loaders = {TBUILD   : self.tpt,
                        FASTLOAD : self.fastload}

    #####################
    # Private Functions #
    #####################
    def _readLoadParameters(self, sectionName):
        '''
        Read parameters for loading
        '''
        parameters = {}
        parameters[LOADER] = self._readMandatoryOptionDefault(sectionName, LOADER)
        parameters[DATABASE] = self._readMandatoryOptionDefault(sectionName, DATABASE)
        parameters[TABLE] = self._readMandatoryOptionDefault(sectionName, TABLE)
        parameters[SOURCE_DIRECTORY] = self._readDirMandatoryOption(sectionName, SOURCE_DIRECTORY)
        defaultFilePattern = "%s_%s*.txt" % (parameters[DATABASE], parameters[TABLE])
        parameters[FILE_PATTERN] = self._readPatternOption(sectionName, FILE_PATTERN, defaultFilePattern)
        parameters[DELIMITER] = self._readOptionDefault(sectionName, DELIMITER, DEFAULT_DELIMETER)
        parameters[CHARACTER_SET] = self._readOptionDefault(sectionName, CHARACTER_SET, 'utf8').upper()
        parameters[MODE] = self._readOptionDefault(sectionName, MODE, None)
        parameters[PRIMARY_KEY] = self._readOptionDefault(sectionName, PRIMARY_KEY, None)
        for key in parameters:
            logging.info("parameters['%s']:%s" % (key, parameters[key]))
        return parameters

    ####################
    # Public Functions #
    ####################
    def simpleLoad(self):
        '''
        USAGE: etl.py --action=simpleLoad --cfg=<config> --section=<section_pattern>
        It will read the options of each section matched <section_pattern> in conf/actions/simpleLoad/<config>.cfg
        EXAMPLE: python2.7 etl.py --action=simpleLoad --cfg=sample --section=newsletter*
        '''
        sections = self.getSectionsForAction(SIMPLE_LOAD_ACTION)

        notLoadedFiles = []
        for section in sections:
            logging.info("\nStart processing section: %s" % section)
            # Read parameters for loading
            parameters = self._readLoadParameters(section)

            # Get files to be loaded
            matchingFiles = self._getMatchingFiles(parameters[SOURCE_DIRECTORY], parameters[FILE_PATTERN])
            if parameters[MODE] == MODE_FULL and len(set([i.split('.')[0] for i in matchingFiles])) > 1:
                msg = "More than one source data found. FULL mode allows only one data file or 2 files set for lob type!"
                raise Exception(msg)

            # Load files one by one
            for file in matchingFiles:
                logging.info("\nStart loading file: '%s'" % file)
                self.loader = self.loaders[parameters[LOADER]]
                parameters[FILENAME] = os.path.basename(file)
                returnCode = self.loader.loadFromFile(parameters)
                if returnCode != RC_NO_ERROR:
                    logging.error("'%s' failed with return code %s." % (parameters[LOADER], returnCode))
                    notLoadedFiles.append(file)
                    continue
                logging.info("The file '%s' was loaded by '%s' successfully." % (file, parameters[LOADER]))
                # Delete source data file and directory for lob data
                os.unlink(file)
                logging.info("The source data file '%s' was removed." % (file))
                if (file.split('.')[-1].lower() == 'lob'):
                    columnFilesDir = file + '_dir'
                    self._removeWorkDirectory(columnFilesDir)
                    logging.info("The lob data directory '%s' was removed." % (columnFilesDir))

        if not notLoadedFiles:
            logging.info("\nThe '%s' action finished successfully." % SIMPLE_LOAD_ACTION)
            return RC_NO_ERROR
        for errFile in notLoadedFiles:
            logging.error("Failed to load file: '%s'" % errFile)
        return RC_ERROR
