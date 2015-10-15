import os
from modules.base.etl_base import EtlBase
from modules.base.constants import *

class Lockfile(EtlBase):
    def __init__(self, action):
        super(Lockfile, self).__init__()
        self.action = action

    #####################
    # Private Functions #
    #####################
    def _getLockfileFullPath(self, lockfileName=None):
        lockfileDir = self._readMandatoryOption(SECTION_DIRECTORIES, LOCKFILE_DIR)
        if not lockfileName:
            return os.path.join(lockfileDir, str(self.action)+'.lock')
        else:
            return os.path.join(lockfileDir, lockfileName+'.lock')

    ####################
    # Public Functions #
    ####################
    def create(self, lockfileName=None):
        '''
        Create a lock file with the specified name. If no name is provided the calling action name will be used
        '''
        lockfileFullPath = self._getLockfileFullPath(lockfileName)
        open(lockfileFullPath, 'w')
        return lockfileFullPath

    def delete(self, lockfileName=None):
        '''
        Delete a lock file with the specified name. If no name is provided the calling action name will be used
        '''
        os.unlink(self._getLockfileFullPath(lockfileName))
        return RC_NO_ERROR

    def exists(self, lockfileName=None):
        '''
        Checks if a lock file exists. Returns True or False.
        '''
        return os.path.exists(self._getLockfileFullPath(lockfileName))
