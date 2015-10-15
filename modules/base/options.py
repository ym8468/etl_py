from ConfigParser             import SafeConfigParser
from optparse                 import OptionParser
import sys

class OptionsFactory(type):
    '''
    '''
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(OptionsFactory, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Options(object):
    __metaclass__ = OptionsFactory
    def __init__(self):
        parser  = OptionParser()
        self.config  = SafeConfigParser()
        section      = 'option_parser_options'

        # Read the global configuration file
        self.config.read('./conf/optionparser_options.cfg')
        options = self.config.options(section)
        for opt in options:
            optValue           = self.config.get(section, opt)
            (longOpt, helpMsg) = optValue.split('|')
            parser.add_option(longOpt, help=helpMsg)
        (options, args) = parser.parse_args()
        self._options   = vars(options)

    #######################
    #  Private Functions  #
    #######################

    ######################
    #  Public Functions  #
    ######################
    def get(self,optKey):
        optVal = None
        if optKey in self._options.keys():
           optVal = self._options[optKey]
        return optVal

    def hasKey(self, optKey):
        if optKey in self._options.keys():
           return True
        return False

    def set(self, optKey, optVal):
        self._options[optKey] = optVal
        return True
