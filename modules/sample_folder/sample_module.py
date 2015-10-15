# Standard Python Modules
import logging

# Super DB Modules
from modules.base.etl_base import EtlBase
from modules.base.constants import *

class SampleModule(EtlBase):
    '''
    This class is sample module
    '''
    def __init__(self):
        super(SampleModule, self).__init__()
        configFile = self.getModuleConfigFile('sample_module', 'sample_folder')
        self.config.read(configFile)

    #####################
    # Private Functions #
    #####################
    def _samplePrivateFunction(self, parameters):
        '''
        Comment for _samplePrivateFunction
        '''
        logging.info("Calling _samplePrivateFunction in SampleModule.")
        for key in parameters:
            logging.debug("parameters[%s] : %s" % (key, parameters[key]))
        return RC_NO_ERROR

    ####################
    # Public Functions #
    ####################
    def samplePublicFunction(self, parameters):
        '''
        Comment for samplePublicFunction
        '''
        logging.info("Calling samplePublicFunction in SampleModule.")
        return self._samplePrivateFunction(parameters)
