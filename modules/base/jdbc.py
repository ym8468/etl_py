import os
from ConfigParser import SafeConfigParser
import jpype
import logging
import thread

import etl_util
import modules.r10_jdbc as jaydebeapi

# Allow to have an external test_jdbc.cfg for local platform development
PATH_JDBC_CFG = os.path.join(os.path.expanduser("~"), '.etl', 'jdbc.cfg')
if not os.path.isfile(PATH_JDBC_CFG):
    PATH_JDBC_CFG = os.path.join(etl_util.get_app_root_path(), 'conf', 'modules', 'jdbc.cfg')

class JDBCFactory(type):
    '''
    This class is factory for JDBC objects

    JDBCObj
        a dictionary of JDBC objects

        JDBCObj Key:
            (connect, driver, user, password, isolation)

        JDBCObj Value:
            a JDBC object

    Usage of JDBC
      Usage1: The isolation remains default.
        JDBC(connect, driver, user, password)
        In this case, JDBCObj Key is (connect, driver, user, password, '')

      Usage2: The isolation is set with a specified isolation.
        JDBC(connect, driver, user, password, isolation)
        In this case, JDBCObj Key is (connect, driver, user, password, isolation)
    '''

    def __init__(self, *args):
        type.__init__(self, *args)
        self.JDBCObj = {}

    def __call__(self, connect='', driver='', user='', password='',isolation=''):
        '''
        Args:
            connect: connect url
            driver: driver number
            user: user name
            password: password

            isolation:isolation level
                READ UNCOMMITTED
                READ COMMITTED
                REPEATABLE READ
                SERIALIZABLE

        Returns:
            a JDBC object
        '''
        #Close all JDBC connections
        curThreadId=thread.get_ident()
        if connect=='':
            for args in self.JDBCObj:
                logging.debug("Current Thread Id: %s" % curThreadId)
                logging.debug("JDBC obj belongs to: %s" % args[5])
                if curThreadId != args[5]:
                    logging.debug("Skip closing JDBC obj which created in other thread.")
                    continue
                try:
                    logging.debug("Now closing JDBC obj.")
                    self.JDBCObj[args].conn.close()
                except Exception,e:
                    logging.error("error happened when closing the JDBC connection!")
                    raise
            self.JDBCObj = {}
            return

        #Get a connection which would be created newly or already exists.
        args = (connect, driver, user, password, isolation, curThreadId)
        if not args in self.JDBCObj:
            logging.debug("Creating a new JDBC connection.")
            self.JDBCObj[args] = type.__call__(self, connect, driver, user, password,isolation)
        return self.JDBCObj[args]

class JDBC:
    '''
    This class is JDBC Connection

    isolation for MySQL
        READ UNCOMMITTED
        READ COMMITTED
        REPEATABLE READ
        SERIALIZABLE

        if isolation is READ UNCOMMITTED,the following SQL will be executed.
            SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

    isolation for Teradata
        READ UNCOMMITTED
        SERIALIZABLE

        if isolation is READ UNCOMMITTED,the following SQL will be executed.
            SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
    '''
    __metaclass__ = JDBCFactory
    def __init__(self, connect, driver, user, password,isolation=''):
        config_parser = SafeConfigParser()
        config_parser.read(PATH_JDBC_CFG)
        # since it is impossible to call jpype.isThreadAttachedToJVM() before jpype.startJVM()
        # we won't check if JVM is started.
        classpath = config_parser.get("JVM", "classpath")
        javaHome  = config_parser.get("JVM", "java_home")
        if (javaHome[0] != '/'):
            javaHome = "%s/%s" % (etl_util.get_app_root_path(), javaHome)

        args='-Djava.class.path=%s' % classpath
        try:
            os.environ['JAVA_HOME'] = javaHome
            jvm_path                = jpype.getDefaultJVMPath()
        except TypeError:
            logging.error("failed to get default JVM path")
            raise
        if (jpype.isJVMStarted() == False):
            jpype.startJVM(jvm_path, args)
        logging.debug("Connecting with %s to %s with user %s" % (driver, connect, user))
        self.conn = jaydebeapi.connect(driver, connect, user, password)

        #set isolation level
        if isolation!='':

            if 'mysql' in driver:
                sql='SET SESSION TRANSACTION ISOLATION LEVEL %s' % isolation
            elif 'teradata' in driver:
                sql='SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL %s' % isolation

            self._execute(sql)

        # not autocommit
        #self.conn.jconn.setAutoCommit(False)

    #####################
    # Private Functions #
    #####################
    # TODO: DEPRECATED - Will be removed after all references fixed
    def _execute(self, sql, parameters=None):
        curs = self.conn.cursor()
        curs.execute(sql, parameters)
        return curs

    ####################
    # Public Functions #
    ####################
    def execute(self, sql, parameters=None):
        curs = self.conn.cursor()
        curs.execute(sql, parameters)
        return curs

    def insert(self, sql, parameters=None):
        #connection.prepareStatement
        cursor = self.conn.cursor()
        cursor.executeInsert(sql,parameters)
        print dir(self.conn)
        cursor.close()

    def _commit(self):
        self.conn.commit()

    def _rollback(self):
        self.conn.rollback()

    def _close(self):
        self.conn.close()

    def _getConnection(self):
        return self.conn

    def _setAutoCommit(self, autoCommit):
        self.conn.jconn.setAutoCommit(autoCommit)
