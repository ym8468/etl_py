# Standard Python Modules
import datetime
import logging
import os
import re
import shutil
import tarfile
import fnmatch
import zipfile
import uuid

# ETL's Code
from modules.base.etl_base import EtlBase
from modules.base.constants import *
from modules.base.lockfile  import Lockfile

class FilesystemActions(EtlBase):
    """
    File system actions
    """
    #######################
    # Config File Options #
    #######################
    def __init__(self):
        super(FilesystemActions, self).__init__()

    #####################
    # Private Functions #
    #####################
    def _readCopyfileParameters(self, sectionName):
        '''
        Read all parameters for copy action
        '''
        parameters = {}
        # Read source_directory
        parameters[SOURCE_DIRECTORY] = self._readDirMandatoryOption(sectionName, SOURCE_DIRECTORY)
        # Read target_directory
        parameters[TARGET_DIRECTORY] = self._readDirMandatoryOption(sectionName, TARGET_DIRECTORY)
        # Read file pattern
        parameters[FILE_PATTERN] = self._readPatternMandatoryOption(sectionName, FILE_PATTERN)
        # Read target_filename
        parameters[TARGET_FILENAME] = self._readPatternOption(sectionName, TARGET_FILENAME, None)
        # Output parameters
        for key in parameters:
            logging.info("parameters['%s']:%s" % (key, parameters[key]))
        return parameters

    def _generateNewFilename(self, targetFilename, origFilename):
        """
        Performs any necessary substitutions on the given filename
        """
        (newFilename, substitutePerformed)  = self._substituteOriginalName(targetFilename, origFilename)
        while (substitutePerformed):
               (newFilename, substitutePerformed)  = self._substituteOriginalName(newFilename, origFilename)
        return newFilename

    def _substituteOriginalName(self, targetFilename, origFilename):
        '''
        Perform ORIGINAL_NAME_int:int substitutions
        '''
        newFilename         = targetFilename
        origNamePattern     = '<ORIGINAL_NAME'
        origNameStart       = targetFilename.find(origNamePattern)
        substitutePerformed = False
        if (origNameStart != -1):
           origNameEnd  = targetFilename.find('>',origNameStart)
           origNameTag  = targetFilename[origNameStart:origNameEnd]
           parts        = origNameTag.split('_')
           (start,end)  = parts[2].split(':')
           origNameTag += '>'
           newFilename  = newFilename.replace(origNameTag, origFilename[int(start):int(end)])
           substitutePerformed = True
        return (newFilename, substitutePerformed)

    def _generateTargetFilepath(self, sourceFile, targetDir, targetFilepattern):
        sourceFilename = os.path.basename(sourceFile)
        if not targetFilepattern:
            targetFilename = sourceFilename
        else:
            targetFilename = self._generateNewFilename(targetFilepattern, sourceFilename)
        return os.path.join(targetDir, targetFilename)

    ####################
    # Public Functions #
    ####################
    def copyfile(self):
        '''
        USAGE: etl.py --action=copyfile --cfg=<config> --section=<section_pattern>
        It will read the options of each section matched <section_pattern> in conf/actions/copyfile/<config>.cfg
        EXAMPLE: python2.7 etl.py --action=copyfile --cfg=sample_us --section=RDC*
        '''
        sections = self.getSectionsForAction(COPYFILE_ACTION)
        notCopiedFiles = []
        for section in sections:
            logging.info("\nStart processing section: %s" % section)
            # Read parameters for copying files
            parameters = self._readCopyfileParameters(section)

            # Get files to be copied
            matchingFiles = self._getMatchingFiles(parameters[SOURCE_DIRECTORY], parameters[FILE_PATTERN])
            logging.debug("Matched files: %s" % matchingFiles)

            # Copy files one by one
            for file in matchingFiles:
                target = self._generateTargetFilepath(file, parameters[TARGET_DIRECTORY], parameters[TARGET_FILENAME])
                logging.info("Coping file '%s' to '%s'" % (file, target))
                try:
                    shutil.copyfile(file, target)
                except:
                    logging.error("Error happend when copying file '%s'!" % (file))
                    notCopiedFiles.append(file)
                    continue
                logging.info("The file was copied successfully.")
        if not notCopiedFiles:
            logging.info("\nThe '%s' action finished successfully." % COPYFILE_ACTION)
            return RC_NO_ERROR
        for errFile in notCopiedFiles:
            logging.error("Failed to copy file: '%s'" % errFile)
        return RC_ERROR

    def removefile(self):
        '''
        USAGE: etl.py --action=removefile --cfg=<config> --section=<section_pattern>
        It will read the options of each section matched <section_pattern> in conf/actions/removefile/<config>.cfg
        EXAMPLE: python2.7 etl.py --action=removefile --cfg=sample_us --section=backup
        '''
        sections = self.getSectionsForAction(REMOVEFILE_ACTION)
        notRemovedFiles = []
        for section in sections:
            logging.info("\nStart processing section: %s" % section)
            # Read parameters for removing files
            filePattern = self._readPatternMandatoryOption(section, FILE_PATTERN)
            logging.info("%s : %s" % (FILE_PATTERN, filePattern))
            targetDir = self._readDirMandatoryOption(section, TARGET_DIRECTORY)
            logging.info("%s : %s" % (TARGET_DIRECTORY, targetDir))
            recursiveFlgStr = self._readOptionDefault(section, RECURSIVE_FLG, 'false')
            recursiveFlg = True if str(recursiveFlgStr).lower() == 'true' else False
            logging.info("%s : %s" % (RECURSIVE_FLG, recursiveFlg))
            backDays = self._readOptionDefault(section, DAYS_BACK, 0)
            logging.info("%s : %s" % (DAYS_BACK, backDays))

            # Get files to be removed
            matchingFiles = self._getMatchingFiles(targetDir, filePattern, recursiveFlg, backDays)
            logging.debug("Matched files: %s" % matchingFiles)

            # Copy files one by one
            for file in matchingFiles:
                logging.info("Removing file '%s'" % (file))
                try:
                    os.unlink(file)
                except:
                    logging.error("Error happend when removing file '%s'!" % (file))
                    notRemovedFiles.append(file)
                    continue
                logging.info("The file was removed successfully.")
        if not notRemovedFiles:
            logging.info("\nThe '%s' action finished successfully." % REMOVEFILE_ACTION)
            return RC_NO_ERROR
        for errFile in notRemovedFiles:
            logging.error("Failed to remove file: '%s'" % errFile)
        return RC_ERROR

    def existsfile(self):
        '''
        USAGE: etl.py --action=existsfile --cfg=<config> --section=<section_pattern>
        It will read the options of each section matched <section_pattern> in conf/actions/existsfile/<config>.cfg
        EXAMPLE: python2.7 etl.py --action=existsfile --cfg=target_data --section=sample_*
        '''
        sections = self.getSectionsForAction(EXISTSFILE_ACTION)
        ngSections = []
        for section in sections:
            logging.info("\nStart processing section: %s" % section)
            # Read parameters for existsfile action
            filePattern = self._readPatternMandatoryOption(section, FILE_PATTERN)
            logging.info("%s : %s" % (FILE_PATTERN, filePattern))
            targetDir = self._readDirMandatoryOption(section, TARGET_DIRECTORY)
            logging.info("%s : %s" % (TARGET_DIRECTORY, targetDir))
            recursiveFlgStr = self._readOptionDefault(section, RECURSIVE_FLG, 'false')
            recursiveFlg = True if str(recursiveFlgStr).lower() == 'true' else False
            logging.info("%s : %s" % (RECURSIVE_FLG, recursiveFlg))
            # find matching files
            matchingFiles = self._getMatchingFiles(targetDir, filePattern, recursiveFlg)
            logging.info("Found files: %s" % matchingFiles)
            # record sections of which file is not found
            if len(matchingFiles) == 0:
                ngSections.append(section)
        if not ngSections:
            logging.info("All files found.")
            return RC_FILE_EXISTS_OK
        for ngSection in ngSections:
            logging.error("File not found for section '%s'." % ngSection)
        return RC_FILE_EXISTS_NG

    def movefile(self):
        '''
        USAGE: etl.py --action=movefile --cfg=<config> --section=<section_pattern>
        It will read the options of each section matched <section_pattern> in conf/actions/movefile/<config>.cfg
        EXAMPLE: python2.7 etl.py --action=movefile --cfg=sample_us --section=load
        '''
        sections = self.getSectionsForAction(MOVEFILE_ACTION)
        notMovedFiles = []
        for section in sections:
            logging.info("\nStart processing section: %s" % section)
            # Read parameters for moving files
            parameters = self._readCopyfileParameters(section)

            # Get files to be moved
            matchingFiles = self._getMatchingFiles(parameters[SOURCE_DIRECTORY], parameters[FILE_PATTERN])
            logging.debug("Matched files: %s" % matchingFiles)

            # Move files one by one
            for file in matchingFiles:
                target = self._generateTargetFilepath(file, parameters[TARGET_DIRECTORY], parameters[TARGET_FILENAME])
                logging.info("Moving file '%s' to '%s'" % (file, target))
                try:
                    shutil.move(file, target)
                except:
                    logging.error("Error happend when moving file '%s'!" % (file))
                    notMovedFiles.append(file)
                    continue
                logging.info("The file was moved successfully.")
        if not notMovedFiles:
            logging.info("\nThe '%s' action finished successfully." % MOVEFILE_ACTION)
            return RC_NO_ERROR
        for errFile in notMovedFiles:
            logging.error("Failed to move file: '%s'" % errFile)
        return RC_ERROR

    def createLockfile(self):
        '''
        USAGE: etl.py --action=createLockfile --cfg=<config> --section=<section_pattern>
        It will read the options of each section matched <section_pattern> in conf/actions/createLockfile/<config>.cfg
        EXAMPLE: python2.7 etl.py --action=createLockfile --cfg=sample --section=sample
        '''
        sections = self.getSectionsForAction(CREATE_LOCKFILE_ACTION)
        lockedFiles = []
        for section in sections:
            logging.info("\nStart processing section: %s" % section)
            # Read parameters for moving files
            filename = self._readPatternMandatoryOption(section, FILENAME)

            # Create Lockfile
            lock = Lockfile(filename)
            if lock.exists():
                lockedFiles.append(filename+'.lock')
            else:
                try:
                    lock.create()
                except:
                    logging.error("Error happend when creating lock file '%s'!" % (filename+'.lock'))
                    raise
                logging.info("The lock file '%s.lock' was created successfully." % filename)
        if not lockedFiles:
            logging.info("Created all lock files successfully.")
            return RC_NO_ERROR
        for lockedFile in lockedFiles:
            logging.warn("The lock file: '%s' was already existed." % lockedFile)
        return RC_EXISTING_PROCESS_EXISTS

    def deleteLockfile(self):
        '''
        USAGE: etl.py --action=deleteLockfile --cfg=<config> --section=<section_pattern>
        It will read the options of each section matched <section_pattern> in conf/actions/deleteLockfile/<config>.cfg
        EXAMPLE: python2.7 etl.py --action=deleteLockfile --cfg=sample --section=sample
        '''
        sections = self.getSectionsForAction(DELETE_LOCKFILE_ACTION)
        notExistedFiles = []
        for section in sections:
            logging.info("\nStart processing section: %s" % section)
            # Read parameters for moving files
            filename = self._readPatternMandatoryOption(section, FILENAME)

            # Delete Lockfile
            lock = Lockfile(filename)
            if lock.exists():
                try:
                    lock.delete()
                except:
                    logging.error("Error happend when deleting lock file '%s'!" % (filename+'.lock'))
                    raise
                logging.info("The lock file '%s.lock' was deleted successfully." % filename)
            else:
                notExistedFiles.append(filename+'.lock')
        if not notExistedFiles:
            logging.info("Deleted all lock files successfully.")
            return RC_NO_ERROR
        for notExistedFile in notExistedFiles:
            logging.error("There is no lock file '%s' to be deleted." % notExistedFile)
        return RC_ERROR
