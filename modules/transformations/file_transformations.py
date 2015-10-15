# Standard Python Modules
import logging
import os
import shutil

# Super DB Modules
from modules.base.etl_base import EtlBase
from modules.base.constants import *


class FileTransformations(EtlBase):
    '''
    This class for all file transformation related functionalities
    '''

    def __init__(self):
        super(FileTransformations, self).__init__()

    #####################
    # Private Functions #
    #####################
    def _createColumnFile(self, column, columnFilesDir):
        columnFileFullPath = os.path.join(columnFilesDir, str(uuid.uuid4())+'.txt')
        with open(columnFileFullPath, 'w') as columnFile:
            columnFile.write(column)
        return columnFileFullPath

    def _generateTptloadLine(self, line, delimiter, maxLengthMappings):
        if not maxLengthMappings:
            return line
        columns = line[0,-1].split(delimiter)
        for key in maxLengthMappings:
            columns[int(key)] = self._createColumnFile(columns[int(key)], columnFilesDir)
        return delimiter.join(columns) + '\n'

    def _getConcatFromLines(self, lines):
        concat = ""
        for line in lines:
            concat += line[0:-1]
        return concat

    def _isOverLengthLine(self, line, delimiter, maxLengthMappings):
        if not maxLengthMappings:
            return False
        columns = line[0:-1].split(delimiter)
        for key in maxLengthMappings:
            if (len(columns[int(key)]) > int(maxLengthMappings[key])):
                return True
        return False

    def _replaceStr(self, string, replaceMappings, sortedKeys):
        for oldStr in sortedKeys:
            string = string.replace(oldStr, replaceMappings[oldStr])
        return string

    def _truncateStr(self, line, delimiter, maxLengthMappings, characterSet='utf8'):
        if not maxLengthMappings:
            return line
        columns = line[0:-1].split(delimiter)
        for key in maxLengthMappings:
            truncatedColumn = columns[int(key)].decode(characterSet, 'ignore')[0:int(maxLengthMappings[key])]
            columns[int(key)] = truncatedColumn.encode(characterSet)
        return delimiter.join(columns) + '\n'

    def _removeMBC(self, line, prefixLengthMappings):
        if not prefixLengthMappings:
            return line
        for hexPrefix in prefixLengthMappings:
            mbcLength = int(prefixLengthMappings[hexPrefix])
            pos = line.find(hexPrefix)
            while  pos >= 0:
                line = line[:pos] + line[pos+mbcLength:]
                pos = line.find(hexPrefix)
        return line

    ####################
    # Public Functions #
    ####################
    def cleanLinefeed(self, file, parameters):
        '''
        Clean linefeed in columns in file
        '''
        logging.info("Start cleanLinefeed processing")
        delimiter = parameters[DELIMITER]
        columnNum = parameters[COLUMN_NUM]
        newFilename = file + '_cleaned'
        errFilename = file + '_error'
        origFile = open(file, 'r')
        newFile = open(newFilename, 'w')
        errFile = open(errFilename, 'w')
        if not columnNum:
            # Get columnNum from the 1st line
            columnNum = len(origFile.readline().split(delimiter))
            origFile.seek(0)
        tmpLines = []
        for line in origFile.readlines():
            if not line.strip():
                logging.info("discard empty line.")
                continue
            colNum = len(line.split(delimiter))
            if colNum >= int(columnNum):
                for tmpLine in tmpLines:
                    errFile.write(tmpLine)
                tmpLines = []
                if colNum == int(columnNum):
                    newFile.write(line)
                else:
                    errFile.write(line)
            else:
                conLine = self._getConcatFromLines(tmpLines)
                conLine += line
                conColNum = len(conLine.split(delimiter))
                while conColNum > int(columnNum):
                    errFile.write(tmpLines.pop(0))
                    conLine = self._getConcatFromLines(tmpLines)
                    conLine += line
                    conColNum = len(conLine.split(delimiter))
                if conColNum == int(columnNum):
                    newFile.write(conLine)
                    tmpLines = []
                else:
                    tmpLines.append(line)
        for tmpLine in tmpLines:
            errFile.write(tmpLine)
        errFile.close()
        newFile.close()
        origFile.close()
        shutil.move(newFilename, file)
        logging.info("File '%s' was renamed to '%s'." % (newFilename, file))
        logging.info("cleanLinefeed processing finished.")
        if (os.stat(errFilename).st_size == 0):
            os.unlink(errFilename)
            logging.info("Empty error file '%s' was removed." % (errFilename))
            return RC_NO_ERROR
        if (os.stat(file).st_size == 0):
            os.unlink(file)
            logging.info("Empty file '%s' was removed." % (file))
        logging.info("There are error records in file '%s'." % (errFilename))
        return RC_ERROR

    def createLobFiles(self, file, parameters):
        '''
        Create lob files for overlength records
        '''
        logging.info("Start createLobFiles processing")
        overLengthFilename = file + '.lob'
        normalLengthFilename = file + '.normal'
        origFile = open(file, 'r')
        overLengthFile = open(overLengthFilename, 'w')
        normalLengthFile = open(normalLengthFilename, 'w')
        lobFilesDir = overLengthFilename + '_dir'
        self._createWorkDirectory(lobFilesDir)
        for line in origFile.readlines():
            if (self._isOverLengthLine(line, parameters[DELIMITER], parameters[MAX_LENGTH_MAPPINGS])):
                overLengthFile.write(self._generateTptloadLine(line, parameters[DELIMITER], parameters[MAX_LENGTH_MAPPINGS], lobFilesDir))
            else:
                normalLengthFile.write(line)
        normalLengthFile.close()
        overLengthFile.close()
        origFile.close()
        shutil.move(normalLengthFilename, file)
        logging.info("File '%s' was renamed to '%s'." % (normalLengthFilename, file))
        logging.info("createLobFiles processing finished.")
        if (os.stat(overLengthFilename).st_size == 0):
            os.unlink(overLengthFilename)
            logging.info("Empty lob file '%s' was removed." % (overLengthFilename))
            self._removeWorkDirectory(lobFilesDir)
            logging.info("Empty lob data directory '%s' was removed." % (lobFilesDir))
        if (os.stat(file).st_size == 0):
            logging.info("Empty file '%s' was removed." % (file))
            os.unlink(file)
        return RC_NO_ERROR

    def replaceChars(self, file, parameters):
        '''
        Replace characters in file
        '''
        logging.info("Start replaceChars processing")
        newFilename = file + '_replaced'
        origFile = open(file, 'r')
        newFile = open(newFilename, 'w')
        for line in origFile.readlines():
            newline = self._replaceStr(line, parameters[REPLACE_MAPPINGS], parameters[SORTED_KEYS])
            newFile.write(newline)
        newFile.close()
        origFile.close()
        shutil.move(newFilename, file)
        logging.info("replaceChars processing finished.")
        return RC_NO_ERROR

    def truncateColumns(self, file, parameters):
        '''
        Truncate columns in file
        '''
        logging.info("Start truncateColumns processing")
        newFilename = file + '_truncated'
        origFile = open(file, 'r')
        newFile = open(newFilename, 'w')
        for line in origFile.readlines():
            newline = self._truncateStr(line, parameters[DELIMITER], parameters[MAX_LENGTH_MAPPINGS], parameters[CHARACTER_SET])
            newFile.write(newline)
        newFile.close()
        origFile.close()
        shutil.move(newFilename, file)
        logging.info("truncateColumns processing finished.")
        return RC_NO_ERROR

    def removeMBC(self, file, parameters):
        '''
        Remove Multiple Bytes Characters in file
        '''
        logging.info("Start removeMBC processing")
        newFilename = file + '_removedMBC'
        origFile = open(file, 'r')
        newFile = open(newFilename, 'w')
        for line in origFile.readlines():
            newline = self._removeMBC(line, parameters[PREFIX_LENGTH_MAPPINGS])
            newFile.write(newline)
        newFile.close()
        origFile.close()
        shutil.move(newFilename, file)
        logging.info("removeMBC processing finished.")
        return RC_NO_ERROR
