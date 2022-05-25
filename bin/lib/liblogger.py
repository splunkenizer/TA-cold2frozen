import os
import logging, logging.handlers
import splunk

def setup_logging(name):

    ### Logging examples
    # logger.debug('debug message')
    # logger.info('info message')
    # logger.warning('warn message')
    # logger.error('error message')
    # logger.critical('critical message')

    # INSTANCE_NAME = os.path.basename(__file__).split(".")[0].lower()
    LOGFILE_NAME = 'cold2frozen'
    logger = logging.getLogger(name)
    SPLUNK_HOME = os.environ['SPLUNK_HOME']
    LOGGING_DEFAULT_CONFIG_FILE = os.path.join(SPLUNK_HOME, 'etc', 'log.cfg')
    LOGGING_LOCAL_CONFIG_FILE = os.path.join(SPLUNK_HOME, 'etc', 'log-local.cfg')
    LOGGING_STANZA_NAME = 'python'
    LOGGING_FILE_NAME = "%s.log" % LOGFILE_NAME
    BASE_LOG_PATH = os.path.join('var', 'log', 'splunk')
    splunk_log_handler = logging.handlers.RotatingFileHandler(os.path.join(SPLUNK_HOME, BASE_LOG_PATH, LOGGING_FILE_NAME), mode='a')
    # Use the same format as splunkd.log
    LOGGING_FORMAT = logging.Formatter("%(asctime)s %(levelname)-s  %(module)s - %(message)s", "%m-%d-%Y %H:%M:%S.%03d %z")
    splunk_log_handler.setFormatter(LOGGING_FORMAT)
    logger.addHandler(splunk_log_handler)
    splunk.setupSplunkLogger(logger, LOGGING_DEFAULT_CONFIG_FILE, LOGGING_LOCAL_CONFIG_FILE, LOGGING_STANZA_NAME)
    return logger