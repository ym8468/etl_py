#!/usr/bin/env python2.7
import logging

class CustomFormatter(logging.Formatter):

    """
    This class is for Custom log formatter
    """

    dbg_fmt  = '%(msg)s'
    info_fmt = '%(msg)s'
    warn_fmt = '[%(levelname)-7s] %(asctime)s %(msg)s'
    err_fmt  = '*************************************[ %(levelname)s ]*************************************\n' + \
                '%(asctime)s (%(filename)s,%(lineno)d) %(msg)s\n' + \
                '*************************************[ %(levelname)s ]*************************************'


    def __init__(self, fmt="%(levelno)s: %(msg)s"):
        logging.Formatter.__init__(self, fmt)

    def format(self, record):
        # Preparation Custom Formatter
        format_org = self._fmt
        if record.levelno == logging.DEBUG:
            self._fmt = CustomFormatter.dbg_fmt

        elif record.levelno == logging.INFO:
            self._fmt = CustomFormatter.info_fmt

        elif record.levelno == logging.WARN:
            self._fmt = CustomFormatter.warn_fmt

        elif record.levelno == logging.ERROR:
            self._fmt = CustomFormatter.err_fmt
        result = logging.Formatter.format(self, record)
        self._fmt = format_org
        return result
