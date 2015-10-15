import logging
import socket
import os

from modules.base.constants  import *
from ConfigParser            import NoOptionError

class EnvironmentFactory(type):
    '''
    A meta class for Environment
    '''
    def __init__(self, *args):
        type.__init__(self, *args)
        self.EnvironmentObj = None

    def __call__(self, config):
        #Get a connection which would be created newly or already exists.
        if self.EnvironmentObj is None:
            self.EnvironmentObj = type.__call__(self, config)

        return self.EnvironmentObj

class Environment:
    '''
    A class for getting environment
    '''
    __metaclass__ = EnvironmentFactory
    def __init__(self,config):
        (self.environment, self.environmentId) = self._getEnvironment(config)

    def _getEnvironment(self,config):
        environment   = "production"
        environmentId = ''

        # Set the Environment ID based on what server we are looking at
        officeServers = ''
        try:
            officeServers  = config.get(SECTION_SERVERS, OFFICE_SERVERS)
        except NoOptionError as exception:
            print "WARNING: Your config file doesn't have any Office Servers set"

        logging.debug("Office Servers are set to '%s'" % (officeServers))

        developmentServers = config.get(SECTION_SERVERS, DEVELOPMENT_SERVERS)
        logging.debug("Development Servers are set to '%s'" % (developmentServers))

        stagingServers     = config.get(SECTION_SERVERS, STAGING_SERVERS)
        logging.debug("Staging Servers are set to '%s'" % (stagingServers))

        currentHostname = socket.gethostname()
        logging.debug("Current Hostname is set to '%s'" % (currentHostname))

        # Check if this is an office server
        if currentHostname in officeServers.split(','):
           logging.debug("'%s' is classified as an office server" % (currentHostname))
           environment = 'office'
           environmentId = 'DEV_'

        # Check if this is a development server
        if currentHostname in developmentServers.split(','):
           logging.debug("'%s' is classified as a development server" % (currentHostname))
           environment = 'development'
           environmentId = 'DEV_'

        # Check if this is a staging server
        if currentHostname in stagingServers.split(','):
           logging.debug("'%s' is classified as a staging server" % (currentHostname))
           environment = 'staging'
           environmentId = 'DEV_'

        # Check environment for ENV_ID and if found override the environment
        if (os.environ.has_key('ENV_ID') == True):
            logging.debug("ENV_ID has been set in the environment and will be used as an override value")
            environmentId = os.environ['ENV_ID']

        logging.debug("Environment is set to '%s'" % (environment))
        logging.debug("Environment ID  (ENV_ID) is set to '%s'" % (environmentId))
        return environment, environmentId
