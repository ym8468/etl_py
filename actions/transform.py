# Standard Python Modules
import logging
import os

# Super DB Modules
from modules.base.constants import *
from modules.base.etl_base import EtlBase
from modules.transformations.file_transformations import FileTransformations

class TransformActions(EtlBase):
    '''
    This class is for all transformation related actions
    '''
    ###############
    # Initializer #
    ###############
    def __init__(self):
        super(TransformActions, self).__init__()
        self.fileTransformations = FileTransformations()

    #####################
    # Private Functions #
    #####################
    def _readTransformParameters(self, sectionName):
        '''
        Read all parameters for transform related action
        '''
        parameters = {}
        # Read source_directory
        parameters[SOURCE_DIRECTORY] = self._readDirMandatoryOption(sectionName, SOURCE_DIRECTORY)
        # Read file pattern
        parameters[FILE_PATTERN] = self._readPatternMandatoryOption(sectionName, FILE_PATTERN)
        # Read transformation_names
        transformationNames = self._readOptionDefault(sectionName, TRANSFORMATION_NAMES, '')
        parameters[TRANSFORMATION_NAMES] = self.getListFromString(transformationNames)
        # Read max_length_mappings
        maxLengthMappings = self._readOptionDefault(sectionName, MAX_LENGTH_MAPPINGS, '')
        parameters[MAX_LENGTH_MAPPINGS] = self.getDictionaryFromString(maxLengthMappings)
        # Read column_num
        parameters[COLUMN_NUM] = self._readOptionDefault(sectionName, COLUMN_NUM, 0)
        # Read encoding
        parameters[CHARACTER_SET] = self._readOptionDefault(sectionName, CHARACTER_SET, 'utf8')
        # Output parameters
        for key in parameters:
            logging.info("parameters['%s']:%s" % (key, parameters[key]))
        # Read delimiter
        delimiter = self._readOptionDefault(sectionName, DELIMITER, DEFAULT_DELIMETER)
        logging.info("parameters['%s']:%s" % (DELIMITER, delimiter))
        parameters[DELIMITER] = delimiter.decode('string_escape')
        # Read replaceMappings and create sorted keys
        replaceMappings = self._readOptionDefault(sectionName, REPLACE_MAPPINGS, '')
        logging.info("parameters['%s']:%s" % (REPLACE_MAPPINGS, replaceMappings))
        replaceMappings = replaceMappings.decode('string_escape')
        # Read prefixLengthMappings
        prefixLengthMappings = self._readOptionDefault(sectionName, PREFIX_LENGTH_MAPPINGS, '')
        logging.info("parameters['%s']:%s" % (PREFIX_LENGTH_MAPPINGS, prefixLengthMappings))
        prefixLengthMappings = prefixLengthMappings.decode('string_escape')
        # Read replace mapping delimiter
        dictDelimiter = self._readOptionDefault(sectionName, 'dict_delimiter', ',')
        logging.info("'%s':%s" % ('dict_delimiter', dictDelimiter))
        dictDelimiter = dictDelimiter.decode('string_escape')
        parameters[REPLACE_MAPPINGS] = self.getDictionaryFromString(replaceMappings, delimiter=dictDelimiter)
        parameters[SORTED_KEYS] = sorted([i for i in parameters[REPLACE_MAPPINGS]], key=len, reverse=True)
        parameters[PREFIX_LENGTH_MAPPINGS] = self.getDictionaryFromString(prefixLengthMappings, delimiter=dictDelimiter)
        return parameters

    def _transformFile(self, file, parameters):
        '''
        Process the parameters for sample action
        '''
        for transformationName in parameters[TRANSFORMATION_NAMES]:
            transformationMethod = getattr(self.fileTransformations, transformationName)
            returnCode = transformationMethod(file, parameters)
            if returnCode != RC_NO_ERROR:
                logging.error("Transformation '%s' failed with return code %s." % (transformationName, returnCode))
                return returnCode
            logging.info("Transformation '%s' completed successfully." % (transformationName))
        return RC_NO_ERROR

    ####################
    # Public Functions #
    ####################
    def transform(self):
        '''
        USAGE: etl.py --action=transform --cfg=<config> --section=<section_pattern>
        It will read the options of each section matched <section_pattern> in conf/actions/transform/<config>.cfg
        '''
        self._validateOptions(['cfg','section'],{})
        configFilename = self.options.get('cfg')
        sectionPattern = self.options.get('section')
        configFile = self.getActionConfigFile(configFilename, TRANSFORM_ACTION)
        sections = self.getSectionsToBeProcessed(configFile, sectionPattern)
        notLoadedFiles = []
        for section in sections:
            logging.info("\nStart processing section: %s" % section)
            # Read parameters for transforming
            parameters = self._readTransformParameters(section)

            # Get files to be transformed
            matchingFiles = self._getMatchingFiles(parameters[SOURCE_DIRECTORY], parameters[FILE_PATTERN])
            logging.info("matchded files: %s" % matchingFiles)

            # Transform files one by one
            for file in matchingFiles:
                logging.info("\nStart transforming file: '%s'" % file)
                returnCode = self._transformFile(file, parameters)
                if returnCode != RC_NO_ERROR:
                    logging.error("Transform failed with return code %s." % (returnCode))
                    notLoadedFiles.append(file)
                    continue
                logging.info("The file '%s' was transformed successfully." % (file))

        if not notLoadedFiles:
            logging.info("\nThe '%s' action finished successfully." % TRANSFORM_ACTION)
            return RC_NO_ERROR
        for errFile in notLoadedFiles:
            logging.error("Failed to transform file: '%s'" % errFile)
        return RC_ERROR
