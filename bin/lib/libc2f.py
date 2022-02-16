import os, sys
import logging, logging.handlers
import splunk

def verify_splunk_home():
    # Verify SPLUNK_HOME environment variable is available, the script is expected to be launched by Splunk which
    #  will set this for debugging or manual run, please set this variable manually
    try:
        os.environ["SPLUNK_HOME"]
    except KeyError:
        print('The environment variable SPLUNK_HOME could not be verified, if you want to run this script '
                    'manually you need to export it before processing')
        sys.exit(1)
    SPLUNK_HOME = os.environ['SPLUNK_HOME']

class c2f:
    def __init__(self):
        self.logger = self.setup_logging()

    def setup_logging(self):

        ### Logging examples
        # logger.debug('debug message')
        # logger.info('info message')
        # logger.warning('warn message')
        # logger.error('error message')
        # logger.critical('critical message')

        # INSTANCE_NAME = os.path.basename(__file__).split(".")[0].lower()
        INSTANCE_NAME = 'cold2frozen'
        logger = logging.getLogger('splunk.%s' % INSTANCE_NAME)
        SPLUNK_HOME = os.environ['SPLUNK_HOME']
        LOGGING_DEFAULT_CONFIG_FILE = os.path.join(SPLUNK_HOME, 'etc', 'log.cfg')
        LOGGING_LOCAL_CONFIG_FILE = os.path.join(SPLUNK_HOME, 'etc', 'log-local.cfg')
        LOGGING_STANZA_NAME = 'python'
        LOGGING_FILE_NAME = "%s.log" % INSTANCE_NAME
        BASE_LOG_PATH = os.path.join('var', 'log', 'splunk')
        splunk_log_handler = logging.handlers.RotatingFileHandler(os.path.join(SPLUNK_HOME, BASE_LOG_PATH, LOGGING_FILE_NAME), mode='a')
        # Use the same format as splunkd.log
        LOGGING_FORMAT = logging.Formatter("%(asctime)s %(levelname)-s  %(module)s - %(message)s", "%m-%d-%Y %H:%M:%S.%03d %z")
        splunk_log_handler.setFormatter(LOGGING_FORMAT)
        logger.addHandler(splunk_log_handler)
        splunk.setupSplunkLogger(logger, LOGGING_DEFAULT_CONFIG_FILE, LOGGING_LOCAL_CONFIG_FILE, LOGGING_STANZA_NAME)
        return logger

    def read_config(self, app_path):
        import configparser

        # Discover app path
        # app name
        APP = os.path.basename(app_path)
        self.logger.debug('Appname: %s' % APP)
        CONFIG_FILE = "cold2frozen.conf"

        # Get config
        config = configparser.RawConfigParser()
        default_config_inifile = os.path.join(app_path, "default", CONFIG_FILE)
        config_inifile = os.path.join(app_path, "local", CONFIG_FILE)

        # First read default config
        self.logger.debug('Reading config file: %s' % default_config_inifile)
        config.read(default_config_inifile)

        # Get default allowed custom values
        ARCHIVE_DIR = config.get("cold2frozen", "ARCHIVE_DIR")
        RESTORE_DIR = config.get("cold2frozen", "RESTORE_DIR")

        # Check config exists
        if not os.path.isfile(config_inifile):
            msg = 'Please configure your setting by creating and configuring %s' % (config_inifile)
            self.logger.error(msg)
            sys.exit(msg)    

        # Then read local config
        self.logger.debug('Reading config file: %s' % config_inifile)
        config.read(config_inifile)

        return config


class logDict(dict):
    # __init__ function 
    def __init__(self): 
        self.__logevent = dict() 
          
    # Function to add key:value 
    def add(self, key, value): 
        self.__logevent[key] = value 

    # Function to return kv list of fields
    def kvout(self):
        kvarray = []
        for key, value in self.__logevent.items():
            kvarray.append("%s=%s" % (key, value))
        return ", ".join(kvarray)
