# Standard Python Modules
import logging

# Super DB Modules
from modules.base.constants import *
from modules.base.etl_base import EtlBase
from modules.sample_folder.sample_module import SampleModule

class SampleActions(EtlBase):
    '''
    This is a sample class
    USAGE: etl.py --action=xxx --cfg=xxx --section=xxx
    '''
    def __init__(self):
        super(SampleActions, self).__init__()
        self.sampleModule = SampleModule()

    #####################
    # Private Functions #
    #####################
    def _readSampleParameters(self, section):
        '''
        Read all parameters for sample action (Usually include parameter validation and initialization)
        '''
        parameters = {}
        # The capital constants SAMPLE_MANDATORY_OPTION and SAMPLE_OPTION are defined in modules/base/constants.py
        # The _readMandatoryOption and _readOption function are in modules/base/etl_base.py
        parameters[MANDATORY_OPTION] = self._readMandatoryOption(section, MANDATORY_OPTION)
        parameters[OPTION] = self._readOption(section, OPTION, 'default_value')
        for key in parameters:
            logging.info("parameters['%s']:%s" % (key, parameters[key]))
        return parameters

    def _processSample(self, parameters):
        '''
        Process the parameters for sample action
        '''
        logging.info("Now calling process function in SampleModule.")
        returnCode = self.sampleModule.samplePublicFunction(parameters)
        logging.info("Processing finished by SampleModule.")
        return returnCode

    ####################
    # Public Functions #
    ####################
    def sample(self):
        '''
        USAGE: etl.py --action=sample --cfg=sample_config --section=<section>
        It will read the options of <section> in conf/actions/sample/sample_config.cfg
        '''
        # The _validateOptions, getActionConfigFile and getSectionsToBeProcessed function are in modules/base/etl_base.py
        self._validateOptions(['cfg','section'], {'cfg':['sample_config'],'section':['section1','section2','section3','section*']})
        configFilename = self.options.get('cfg')
        sectionPattern = self.options.get('section')
        configFile = self.getActionConfigFile(configFilename, SAMPLE_ACTION)
        sections = self.getSectionsToBeProcessed(configFile, sectionPattern)
        errSections = []
        for section in sections:
            logging.info("\nStart processing section: %s" % section)
            parameters = self._readSampleParameters(section)
            returnCode = self._processSample(parameters)
            if returnCode != RC_NO_ERROR:
                logging.error("The process module failed with return code %s." % (returnCode))
                errSections.append(section)
            logging.info("The process module completed successfully.")
        if not errSections:
            logging.info("\nThe '%s' action finished successfully." % (SAMPLE_ACTION))
            return RC_NO_ERROR
        for errSection in errSections:
            logging.error("Error happened when processing section: '%s'" % (errSection))
        return RC_ERROR
