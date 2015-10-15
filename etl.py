#!/usr/bin/env python2.7
from subprocess               import Popen
from ConfigParser             import SafeConfigParser
from modules.base.exceptions  import FileConfigMissing,MandatoryOptionMissing
from modules.base.jdbc        import JDBC
from modules.base.lockfile    import Lockfile
from modules.base.options     import Options

import datetime
import inspect
import logging
import os.path
import re
import sys
import traceback
import uuid

###########################################################################
# Author   : meng.ye                                                      #
# Language : Python2.7.x                                                  #
# Purpose  : Create a lightweight python script that can be reused easily #
###########################################################################
def _findCallableActions(aClass, classDescription, validActions):
    for name, data in inspect.getmembers(aClass):
        if re.match('_',name):
            continue
        func = getattr(aClass, name, None)
        if callable(func):
            validActions[name] = (func,classDescription)
    return validActions

def  _readApplicationConfigurationFiles():
    '''
    This function is responsible for initially reading the configuration information for the application
    '''
    configParser = SafeConfigParser()
    configParser.read('conf/globals.cfg')
    return configParser

def _convertToCamelcase(value):
    def camelcase():
        yield str.capitalize
        while True:
           yield str.capitalize

    c = camelcase()
    return "".join(c.next()(x) if x else '_' for x in value.split("_"))

def main():
    options = Options()

    # Read global configuration file
    configParser    = _readApplicationConfigurationFiles()

    #####################
    # Configure logging #
    #####################
    # Determine ML_JOBNET_NAME
    jobnet_name = ''
    if os.environ.has_key('ML_JOBNET_NAME'):
        jobnet_name = os.environ['ML_JOBNET_NAME']

    # Create log directory if it does not exist
    base_log_directory = configParser.get('directories', 'LOG_DIRECTORY')
    log_directory = "%s/%s" % (base_log_directory, jobnet_name)
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # Determine the log file name
    current_date = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    if os.access(log_directory, os.W_OK):
        logFilename = "%s/etl-%s.%s_%s.log" % (log_directory, options.get('action'), current_date, str(uuid.uuid4()))
    else:
        logFilename = "work/etl-%s.%s_%s.log" % (options.get('action'), current_date, str(uuid.uuid4()))

    # define a handler which writes INFO messages or higher
    console_log = logging.StreamHandler()
    console_log.setLevel(logging.INFO)

    # define a handler which writes DEBUG messages or higher
    file_log = logging.FileHandler(filename=logFilename)
    file_log.setLevel(logging.DEBUG)
    file_format = logging.Formatter('%(asctime)s [%(levelname)-7s] (%(filename)s,%(lineno)d) %(message)s')
    file_log.setFormatter(file_format)

    logging.getLogger().addHandler(console_log)
    logging.getLogger().addHandler(file_log)
    logging.getLogger().setLevel(logging.DEBUG)

    ############################
    # Dynamically Load Modules #
    ############################
    actionLabels = {'actions.py':'General Actions'}
    validActions = {}
    modules      = {}
    for file in os.listdir('actions'):
        if (file.strip().endswith('.py') and file.strip() != '__init__.py'):
            # Dynamically Load the Module
            moduleName    = file.split('.py')[0]
            className     = "%sActions" % (_convertToCamelcase(moduleName))
            modulePath    = "actions.%s" % (moduleName)
            _temp = __import__(modulePath, globals(), locals(), ['object'], -1)

            # Dynamically get an instance of the given class
            classInstance = getattr(_temp, className)()

            # Find all executable actions
            validActions = _findCallableActions(classInstance, moduleName, validActions)

    # Validate if the action is valid. Print valid actions if no valid action is found
    if (options.get('action') not in validActions):
        logging.warn("%s is not a valid action. Please specify an action from the list below" % (options.get('action')))
        actions = validActions.keys()

        # Group All Actions by their class descriptions
        actionsByClass = {}
        for action in actions:
            classDescription = validActions[action][1]
            if (actionsByClass.has_key(classDescription) == True):
               tmp = actionsByClass[classDescription]
               tmp.append(action)
               actionsByClass[classDescription] = tmp
            else:
               actionsByClass[classDescription] = [action]

        for classDesc in actionsByClass:
            print "\n%s" % (classDesc)
            print "----------------------"
            actions = actionsByClass[classDesc]
            actions.sort()
            for action in actions:
               print action
        sys.exit(197)

    # Determine the JC_JOBID Environment Value
    if (os.environ.has_key('JC_JOBID')):
       jcJobId = os.environ['JC_JOBID']
    else:
       jcJobId = ''

    # Check if a lock file exists, exit if it does
    lockfile = Lockfile(options.get('action'))
    if (lockfile.exists() == True):
       logging.warn('Action is currently running!')
       sys.exit(198)

    # Log the start of the process
    msg = "Starting the '%s' action" % (options.get('action'))
    logging.debug(msg)

    # Call the requested action, if an error occurs, log the error and send an email
    try:
       func       = validActions[options.get('action')][0]
       returnCode = func()
    except FileConfigMissing as fcm:
       logging.error(fcm)
       returnCode = 101
    except MandatoryOptionMissing as mom:
       logging.error(mom)
       returnCode = 102
    except Exception,e:
       logging.error(e)
       exc_type, exc_value, exc_traceback = sys.exc_info()
       tb                                 = traceback.extract_tb(exc_traceback)
       formattedTb                        = traceback.format_list(tb)
       # All traceback concatenate for output
       tbOutputLine = ''
       for line in formattedTb:
           tbOutputLine = tbOutputLine + line
       logging.error(tbOutputLine)
       if (lockfile.exists() == True):
           lockfile.delete()
       returnCode = 199  #TODO: make this look up the specific action's section and see if a different return code is defined

    finally:
       # Close all JDBC connections
       JDBC()
       msg = "Logfile = '%s'" % (logFilename)
       logging.info(msg)
       msg = "JC_JOBID = '%s'" % (jcJobId)
       logging.info(msg)

    msg = "The action '%s' has finished. Return Code is %s" % (options.get('action'), returnCode)
    logging.info(msg)

    sys.exit(returnCode)

main()
