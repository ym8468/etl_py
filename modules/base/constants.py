##############
# Array Keys #
##############
KEY_DATABASE = 'database'

###################
# Extractor Names #
###################
MYSQL                               = 'mysql'
SQLSERVER                           = 'sqlserver'

################
# Loader Names #
################
FASTLOAD                            = 'fastload'
TBUILD                              = 'tbuild'

################
# Action Names #
################
SAMPLE_ACTION                              = 'sample'
SIMPLE_EXTRACT_ACTION                      = 'simpleExtract'
SIMPLE_LOAD_ACTION                         = 'simpleLoad'
TRANSFORM_ACTION                           = 'transform'
COPYFILE_ACTION                            = 'copyfile'
REMOVEFILE_ACTION                          = 'removefile'
MOVEFILE_ACTION                            = 'movefile'
CREATE_LOCKFILE_ACTION                     = 'createLockfile'
DELETE_LOCKFILE_ACTION                     = 'deleteLockfile'
CREATE_TABLE_ACTION                        = 'createTable' 
EXISTSFILE_ACTION                          = 'existsfile'

###############################
# Configuration File Sections #
###############################
SECTION_COMMON                      = 'COMMON'
SECTION_CODEPAGES                   = 'codepages'
SECTION_DEFAULT                     = 'default'
SECTION_DIRECTORIES                 = 'directories'
SECTION_EXTRACT_INFO                = 'extract_info'
SECTION_FEXP_EXTRACT_ACTION         = 'fexpExtract_action'
SECTION_FILES_EXIST                 = "files_exist"
SECTION_FILES                       = "files"
SECTION_GENERATE_REPORT             = 'GENERATE_REPORT'
SECTION_JDBC                        = 'jdbc'
SECTION_MAIL_DEFAULT                = 'MAIL_DEFAULT'
SECTION_OS_COMMANDS                 = 'os_commands'
SECTION_SEND_EMAIL                  = 'send_email'
SECTION_ETL_SCRIPTS                 = 'etl_scripts'
SECTION_SUMMARIZE                   = 'summarize_bteq'
SECTION_TABLE                       = 'table'
SECTION_TERADATA                    = 'TERADATA'
SECTION_TERADATA_DATABASES          = 'teradata_databases'
SECTION_TERADATA_USERS              = 'teradata_users'
SECTION_TARGET_FILE_CONFIGURATION   = 'target_file_configuration'
SECTION_SERVERS                     = 'servers'
SECTION_RED_TERADATA                = 'red_teradata.cf'  # TODO: Remove this completely in the future

######################################
# Configuration Options for Commands #
######################################
OPTION_FASTLOAD_COMMAND                    = 'fastload_command' # TODO: REMOVE, Deprecated
OPTION_GPG_COMMAND                         = 'gpg_command'      # TODO: REMOVE, Deprecated
OPTION_MYSQL_COMMAND                       = 'mysql_command'# TODO: REMOVE, Deprecated
OPTION_PROXY_COMMAND                       = 'proxy_command'# TODO: REMOVE, Deprecated

FASTLOAD_COMMAND                    = 'fastload_command'
GPG_COMMAND                         = 'gpg_command'
MYSQL_COMMAND                       = 'mysql_command'
PROXY_COMMAND                       = 'proxy_command'


#############################################################
# Teradata Password Constants                               #
#############################################################
TERA_PASS_USER_LOGON                    = "USER_LOGON"
TERA_PASS_PASSWORD                      = "PASSWORD"
TERA_PASS_LOGON_USER                    = "LOGON_USER"

#############################
# Password Directory Option #
#############################
MYSQL_PASSWORD_DIRECTORY            = 'password_mysql_directory'
PASSWORD_TERADATA_DIRECTORY         = 'password_teradata_directory'

##############################
# Configuration File Options #
##############################
ACTUAL_DATABASE                     = 'actual_database'
ADD_HEADER                          = 'add_header'
ADDITIONAL_CONDITIONS               = 'additional_conditions'
ADDITIONAL_COLUMNS                  = 'additional_columns'
APPEND_DATA                         = 'append_data'
ARCHIVE_BASE_DIRECTORY              = 'archive_base_directory'
ATTACHMENTS                         = 'attachments'
APPENDMODE                          = 'appendMode'
BACKUP                              = 'backup'
BCCADDRESS                          = 'bcc_address'
BTEQ_TEMPLATE_BASE_DIRECTORY        = 'bteq_template_base_directory'
BTEQ_TEMPLATE_FILE                  = 'bteq_template_file'
CCADDRESS                           = 'cc_address'
CELL_DATE                           = 'cellDate'
CELL_MONTH                          = 'cellMonth'
CELL_YEAR                           = 'cellYear'
CHARACTER_SET                       = 'character_set'
CHECK_INTERVAL                      = 'check_interval'
CHECK_TIMEOUT                       = 'check_timeout'
CODEPAGE_JP_LANG                    = 'jpLang'
CODEPAGE_ENG_JP_LANG                = 'jpNlsLang'
CODEPAGE_ENG_LANG                   = 'engLang'
CODEPAGE_ENG_NLS_LANG               = 'engNlsLang'
COLUMNS                             = 'columns'
COLUMN_NUM                          = 'column_num'
COLUMN_INDEXES                      = 'column_indexes'
COLUMN_NAME                         = 'column_name'
COLUMN_NAMES                        = 'column_names'
COLUMN_TYPE                         = 'column_type'
CONNECT                             = 'connect'
CONSUMER                            = 'consumer'
CONSUME_METHOD                      = 'consume_method'
COUNTRY_SHEET                       = 'countrySheet'
COUNTRY_PREFIX                      = 'countryPrefix'
COUNTRY_ROW                         = 'countryRow'
COUNTRY_CELL                        = 'countryCell'
CREATEMODE                          = 'createMode'
CSV_BASE_FILENAME                   = 'csv_base_filename'
CURRENT_DATE                        = 'current_date'
CURRENT_DATETIME                    = 'current_datetime'
DATABASE                            = 'database'
DATE_FORMAT                         = 'dateFormat'
DATA_START_ROW_INDEX                = 'data_start_row_index'
DATA_TYPE                           = 'data_type'
DATE_STAMP_LOCATION                 = 'date_stamp_location'
DAYS_BACK                           = 'days_back'
DEAD_LINE                           = 'dead_line'
DEFAULT_DAYS_BACK                   = 1
DEFAULT_TIMEZONE                    = 'utc'
DELETE_CHARS                        = 'delete_chars'
DELIMETER                           = 'delimeter'
DELIMITER                           = 'delimiter'
DEVELOPMENT_SERVERS                 = 'development_servers'
DIRECTORY_CONFIG_DATA               = 'conf/data/'
DRIVER                              = 'driver'
EMPTY_STRING                        = 'emptyString'
ENCLOSED_BY                         = 'enclosed_by'
ENDING_COLUMN                       = 'ending_column'
ENDING_ROW                          = 'ending_row'
END_DATE                            = 'end_date'
END_POSITION                        = 'end_position'
ENV_DBNAME                          = 'env_dbname'
EXTRACTOR                           = 'extractor'
FIELDS                              = 'fields'
FIELDS_NUM                          = 'fields_num'
FROMADDRESS                         = 'from_address'
FILENAME                            = 'filename'
FILE_CONFIG_DIRECTORY               = 'file_config_directory'
FILE_PATTERN                        = 'file_pattern'
FILE_PERMISSION                     = 'permission'
FILE_PREFIX                         = 'file_prefix'
FILE_TEMPLATE                       = 'file_template'
FIRST_PARTY_SHEET_NAME              = 'first_party_sheet_name'
FTP_HOST                            = 'ftp_host'
FTP_SERVER_BASE_DIRECTORY           = 'ftp_server_base_directory'
GENERATOR                           = 'generator'
HEADER_FLG                          = 'header_flg'
HEADER_NAMES                        = 'header_names'
HISTORICAL_FLAG                     = 'flag_field'
HISTORICAL_INDEX                    = 'historical_index'
HISTORICAL_TABLE                    = 'historicalTable'
HOST                                = 'host'
HOSTNAME                            = 'host_name'
HOURS_BACK                          = 'hours_back'
HTMLBODY                            = 'htmlBody'
HTML_TEMPLATE                       = 'html_template'
IDENTITY_FILE                       = 'identity_file'
IN_CHARS                            = 'in_chars'
INPUT_DIRECTORY                     = 'input_directory'
JOBNAME                             = 'jobname'
KEYSPACE                            = 'keyspace'
LOAD01_SHELL                        = 'load01_shell'
LOADER                              = 'loader'
LOB_DIR                             = 'lob_dir'
LOB_COLUMN_INDEX                    = 'lob_column_index'
LOG_FILE                            = 'log_file'
ODBC_HOME                           = 'odbcHome'
ODBC_INI                            = 'odbcIni'
ORACLE_BASE                         = 'oracleBase'
ORACLE_HOME                         = 'oracleHome'
ORACLE_TNS_ADMIN                    = 'tnsAdmin'
ORACLE_SID                          = 'oracleSid'
OUT_CHARS                           = 'out_chars'
OUTLOOK_HOSTNAME                    = 'outlook_hostname'
OUTLOOK_MAILBOX                     = 'outlook_mailbox'
OUTLOOK_PASSWORD                    = 'outlook_password'
OUTLOOK_PORT                        = 'outlook_port'
OUTLOOK_USERNAME                    = 'outlook_username'
OUTPUT_DIRECTORY                    = 'output_dir'
OUTPUT_FILE                         = 'output_file'
OUTPUT_FILENAME                     = 'output_filename'
PARAMETERS_DIRECTORY                = 'parameters_directory'
PREFIX_LENGTH_MAPPINGS              = 'prefix_length_mappings'
PRIMARY_INDEX                       = 'primary_index'
PRIMARY_KEY                         = 'primary_key'
PRODUCTION_ENVIRONMENT              = 'production'
LAST_EMAIL_READ                     = 'last_email_read'
MANDATORY_OPTION                    = 'mandatory_option'
MAPPING_INDEX                       = 'mapping_index'
MAPPING_SOURCE                      = 'mapping_source'
MAPPING_TARGET                      = 'mapping_target'
MAPPING_TYPE                        = 'mapping_types'
MARKETPLACE_SHEET_NAME              = 'marketplace_sheet_name'
MAX_LENGTH                          = 'max_length'
MAX_LENGTH_MAPPINGS                 = 'max_length_mappings'
MESSAGE_PARSER                      = 'message_parser'
MODE                                = 'mode'
NEW_STRING                          = 'new_string'
NULL_STRING                         = 'null_string'
NUMBERCOLUMNS                       = 'numberColumns'
NUMBERSHEETS                        = 'numberSheets'
NUMBER_OF_BYTES                     = 'number_of_bytes'
NUMBER_OF_ROWS                      = 'number_of_rows'
OFFICE_SERVERS                      = 'office_servers'
OLD_STRING                          = 'old_string'
OPTION                              = 'option'
OUT_OF_RANGE_DATE                   = 'outOfRangeDate'
PARAMETERS                          = 'parameters'
PASSPHRASE                          = 'passphrase'
PASSWORD                            = 'password'
PASSWORD_GPG_DIRECTORY              = 'password_gpg_directory'
PORT                                = 'port'
PLAINBODY                           = 'plainBody'
PROCESS_METHOD                      = 'process_method'
PROXY_HOST                          = 'proxy_host'
PROXY_PORT                          = 'proxy_port'
RECORD_LENGTH                       = 'record_length'
RECURSIVE_FLG                       = 'recursive_flg'
REGRESSION_COMMAND                  = 'command'
REGRESSION_PATTERN                  = 'pattern'
REGRESSION_POST_COMMAND             = 'postcommand'
REGRESSION_PRE_COMMAND              = 'precommand'
REGRESSION_RESULT                   = 'result'
REMOVE_FLG                          = 'remove_flg'
REPLACE_MAPPINGS                    = 'replace_mappings'
REPLACE_STYLE                       = 'replace_style'
RETURN_CODE                         = 'return_code'
RETRY_INTERVAL                      = 'retry_interval'
RETRY_TIMEOUT                       = 'retry_timeout'
RM_AFTER_GET                        = 'rm_after_get'
RM_AFTER_PUT                        = 'rm_after_put'
ROUTING_KEY                         = 'routing_key'
ROW_MONTH                           = 'rowMonth'
ROW_YEAR                            = 'rowYear'
SCHEMA                              = 'schema'
SECTION_NAME                        = 'section_name'
SESSIONNAME                         = 'ses_name'
SET_LANG                            = 'setLang'
SHEET_DATE                          = 'sheetDate'
SHEET_NAME1                         = 'SheetName1'
SHEET_NAME2                         = 'SheetName2'
SHEET_PREFIX                        = 'sheetPrefix'
SKIP_FILE_ROWS                      = 'skip_file_rows'
SKIPROWS                            = 'skipRows'
SLASH                               = 'slash'
SORTED_KEYS                         = 'sorted_keys'
SOURCE_DIRECTORY                    = 'source_directory'
SOURCE_INDEX                        = 'source_index'
SOURCE_TABLE                        = 'sourceTable'
SQL_FILE                            = 'sql_file'
SQL_TEMPLATE                        = 'sql_template'
SOURCECONNECTION                    = 'DBConnection_Source'
SOURCETABLE                         = 'Param_Src_Tablename'
STAGING_SERVERS                     = 'staging_servers'
STARTCOLUMNS                        = 'startColumns'
STARTING_COLUMN                     = 'starting_column'
STARTING_ROW                        = 'starting_row'
START_DATE                          = 'start_date'
START_POSITION                      = 'start_position'
STRIP_DUPLICATE_DELIMETER           = 'strip_duplicate_delimeter'
STYLE                               = 'style'
SUBJECT                             = 'subject'
SUBSIDUARIES                        = 'subsiduaries'
SUBNAME                             = 'subName'
SUFFIXNAME                          = 'suffixName'
TABLE                               = 'table'
TABLE_NAMES                         = 'table_names'
TABLE_CONFIG_TEMPLATE_FILE          = 'table_config_template_file'
TABLE_DEF                           = 'table_def'
TABLE_USER                          = 'TABLE_USER'
TARGETCONNECTION                    = 'DBConnection_Target'
TARGET_DATE                         = 'target_date'
TARGET_DATE_COLUMN                  = 'target_date_column'
TARGET_DIRECTORY                    = 'target_directory'
TARGET_FILENAME                     = "target_filename"
TARGETTABLE                         = 'Param_Tgt_Tablename'
TDPID                               = 'tdpid'
TEMPLATE_BTEQ_FILE                  = 'template_bteq_file'
TERADATA_HOME                       = 'teradataHome'
TERADATA_CREATE_TABLE_TEMPLATE_FILE = 'teradata_create_table_template_file'
TERADATA_FASTEXPORT_TEMPLATE_FILE   = 'teradata_fastexport_template_file'
TERADATA_FASTLOAD_TEMPLATE_FILE     = 'teradata_fastload_template_file'
TERADATA_FASTLOAD2_TEMPLATE_FILE    = 'teradata_fastload2_template_file'
TERADATA_BTEQLOAD_TEMPLATE_FILE     = 'teradata_bteqload_template_file'
TERADATA_BTEQLOAD2_TEMPLATE_FILE    = 'teradata_bteqload2_template_file'
TERADATA_BTEQLOAD_FULLMODE_TEMPLATE_FILE = 'teradata_bteqload_fullmode_template_file'
TERADATA_TPTINSERTER_TEMPLATE_FILE  = 'teradata_tptinserter_template_file'
TERADATA_TPTLOAD_TEMPLATE_FILE      = 'teradata_tptload_template_file'
TERADATA_TPTLOAD_FULLMODE_TEMPLATE_FILE  = 'teradata_tptload_fullmode_template_file'
TERADATA_UPDATE_SCHEME_TEMPLATE_FILE = 'teradata_update_scheme_template_file'
TERADATA_DELETE_TABLE_TEMPLATE_FILE  = 'teradata_delete_table_template_file'
UPDATE_TABLE_SCHEME_CFG_FILE        = 'update_table_scheme_cfg_file'
DELETE_TABLES_CFG_FILE              = 'delete_tables_cfg_file'
TEXT_TEMPLATE                       = 'text_template'
TIMEOUT                             = 'timeout'
TIMEZONE                            = 'timezone'
TMP_TABLE                           = 'tmp_table'
TOADDRESS                           = 'to_address'
TODAY                               = 'today'
YESTERDAY                           = 'yesterday'
TRANSFORMATION_NAMES                = 'transformation_names'
TRANSFORM_COLUMN                    = 'transform_column'
TRANSFORM_COLUMN_METHODS            = 'transform_column_method'
TRANSFORM_TARGET_COLUMN             = 'transform_target_column'
TRANSFORM_TARGET_COLUMN_METHODS     = 'transform_target_column_method'
TYPE                                = 'type'
ADD_COLUMN                          = 'add_column'
ADD_COLUMN_METHOD                   = 'add_column_method'
USER                                = 'user'
USERNAME                            = 'username'
VARIABLES                           = 'variables'
VHOST                               = 'vhost'
WHERE_CLAUSE                        = 'where_clause'
WORK_FILE                           = 'work_file'
WORKFILENAME                        = 'work_filename'
WORKFLOWNAME                        = 'wf_name'
QUEUE                               = 'queue'
QUOTE_CHAR                          = 'quote'
PARALLEL_LOAD_PASS                  = 'parallel_load_pass'

##########################
# Extraction Mode Values #
##########################
MODE_FULL            = 'full'
MODE_DAYS_BACK       = 'days_back'
MODE_PROCESS_HISTORY = 'process_history'
DEFAULT_MODE         = MODE_DAYS_BACK
SIMPLE               = 'simple'

############################
# Delete Table Mode Values #
############################
BACKUP_CHECK = 'backup_check'
NO_CHECK = 'no_check'

############################
# Fast Export Record Length#
############################
DEFAULT_RECORD_LENGTH = 1000

####################
# Delimeter Values #
####################
DELIMETER_POINT                = '.'
DELIMETER_MINUS                = '-'
DELIMETER_COMMA                = ','
DELIMETER_PIPE                 = '|'
DELIMETER_TERADATA_RECOMMENDED = '&@!&'
DEFAULT_DELIMETER = DELIMETER_TERADATA_RECOMMENDED
DELIMETER_TAB                  = '\t'


##########################
# Extract Replace Styles #
##########################
MYSQL_REPLACEMENT       = 'mysql'
DEFAULT_EXTRACT_REPLACE = MYSQL_REPLACEMENT

###########################################
# Constants for the Daily GMS Spreadsheet #
###########################################
XLRD                         = 'xlrd'
DAILY_GMS_EXCEL_MARKETPLACE  = 'marketplace'
DAILY_GMS_EXCEL_FIRST_PARTY  = 'first_party'
DAILY_GMS_EXCEL_ROW_TAG      = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}row"
DAILY_GMS_EXCEL_C_TAG        = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}c"
DAILY_GMS_EXCEL_F_TAG        = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}f"
DAILY_GMS_EXCEL_T_TAG        = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t"
DAILY_GMS_EXCEL_V_TAG        = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}v"

#####################
# Formatting String #
#####################
FORMATTER_INTEGER    = "{0:20,.0f}"
FORMATTER_PERCENTAGE = "{0:20,.2f}"

##################
# Database Names #
##################
DB_NAME_SAMPLE       = "sample"

###############
# Table Names #
###############
TABLE_SAMPLE = 'sampletable'

###############
# Misc Values #
###############
BTEQ_ENV_SUB_VARIABLE = 'BTEQ_VAR_'
STYLE_DAYS_BACK       = 'days_back'
STYLE_LAST_LOADED     = 'last_loaded'

################
# Return Codes #
################
RC_NO_ERROR                         = 0
RC_ERROR                            = 1
RC_EXISTING_PROCESS_EXISTS          = 1
RC_ERROR_COULD_NOT_CREATE_DIRECTORY = 100

RC_FILE_EXISTS_ALL_FILES_OK                   = 0
RC_FILE_EXISTS_OK                             = 0
RC_FILE_EXISTS_NG                             = 1
RC_FILE_EXISTS_CONFIGURATION_FILE_MISSING     = 101
RC_FILE_EXISTS_SECTION_MISSING                = 102
RC_FILE_EXISTS_INPUT_DIRECTORY_OPTION_MISSING = 103
RC_FILE_EXISTS_INPUT_DIRECTORY_DOES_NOT_EXIST = 104
RC_FILE_EXISTS_EXPECTED_FILES_OPTION_MISSING  = 105

RC_NETWORK_CONNECTION_ERROR    = 110
RC_NETWORK_AUTHORIZATION_ERROR = 111
RC_NETWORK_DOWNLOAD_ERROR      = 112

###################################
# executeBteq Action Return Codes #
###################################
RC_EXECUTE_BTEQ_NORMAL_END          = 0
RC_EXECUTE_BTEQ_FILE_DOES_NOT_EXIST = 1

#TO DO:Modify to appropriate code
RC_AUTOMATED_TEST_ERROR = 1

#########################
# Fastload Return Codes #
#########################
RC_FASTLOAD_NORMAL = 0
ET_COUNT = 'et_count'
UV_COUNT = 'uv_count'
DELETE_COUNT = 'delete_count'
INSERT_COUNT = 'insert_count'

#########################
# Bteq Return Codes #
#########################
RC_BTEQ_NORMAL = 0

#####################################
# Delete Backup Tables Return Codes #
#####################################
RC_DELETE_SKIPPED = 2

############################
# FAST EXPORT RETURN CODES #
############################
FEXP_RETURN_CODE_NORMAL                 = 0
FEXP_RETURN_CODE_WARNING                = 4
FEXP_RETURN_CODE_USER_ERROR             = 8
FEXP_RETURN_CODE_FATAL_ERROR            = 12
FEXP_RETURN_CODE_NO_MESSAGE_DESTINATION = 16

##############################
# AUTOMATED TEST ERROR CODES #
##############################
AUTOTEST_ERRORCODE_PETTY   =10
AUTOTEST_ERRORCODE_EMPTY   =20
AUTOTEST_ERRORCODE_VALUE   =30
AUTOTEST_ERRORCODE_DDL     =40
AUTOTEST_ERRORCODE_DB      =50
AUTOTEST_ERRORCODE_CONFIG  =60
AUTOTEST_ERRORCODE_OTHER   =90

###########################
# Fastload charset values #
###########################
FLOAD_CHARSET_ASCII='ASCII'                             # Latin Network-attached
FLOAD_CHARSET_EBCDIC='EBCDIC'                           # Latin Channel-attached
FLOAD_CHARSET_HANGULEBCDIC933_1II='HANGULEBCDIC933_1II' # Korean Channel-attached
FLOAD_CHARSET_HANGULKSC5601_2R4='HANGULKSC5601_2R4'     # Korean Network-attached
FLOAD_CHARSET_KANJIEUC_0U='KANJIEUC_0U'                 # Japanese Network-attached
FLOAD_CHARSET_KANJISJIS_0S='KANJISJIS_0S'               # Japanese Network-attached
FLOAD_CHARSET_KATAKANAEBCDIC='KATAKANAEBCDIC'           # Japanese Channel-attached
FLOAD_CHARSET_KANJIEBCDIC5026_0I='KANJIEBCDIC5026_0I'   # Japanese Channel-attached
FLOAD_CHARSET_KANJIEBCDIC5035_0I='KANJIEBCDIC5035_0I'   # Japanese Channel-attached
FLOAD_CHARSET_SCHEBCDIC935_2lJ='SCHEBCDIC935_2lJ'       # Simplified Chinese Channel-attached
FLOAD_CHARSET_SCHGB2312_1T0='SCHGB2312_1T0'             # Simplified Chinese Network-attached
FLOAD_CHARSET_TCHEBCDIC937_3IB='TCHEBCDIC937_3IB'       # Traditional Chinese Channel-attached
FLOAD_CHARSET_TCHBIG5_1R0='TCHBIG5_1R0'                 #

##############################
# Teradata Related Constants #
##############################
TERADATA_RESERVED_WORDS        = ['create','drop','grant','select','insert','update','delete','table','into','from','where','and','or','like','value','values','order','by','on','to','as','asc','desc','having','group','limit','escape','if','else','then','when','case','yes','no','not','password','sum','count','unique','cast','max','min','view','trim','with','type','help','all','set','primary','index','default','multiset','fallback','before','after','journal','checksum','mergeblockratio','character','unicode','casespecific','account','sample','year','month','date','hour','minute','second','error','comment','day','title','row_number','level','operator','next','summary','user']
TERADATA_DATA_TYPE_REPRESENTATIONS = {'AT':'TIME',
                                      'BF':'BYTE',
                                      'BO':'BLOB',
                                      'BV':'VARBYTE',
                                      'CF':'CHAR',
                                      'CO':'CLOB',
                                      'CV':'VARCHAR',
                                      'D' :'DECIMAL',
                                      'DA':'DATE',
                                      'F' :'FLOAT',
                                      'I1':'BYTEINT',
                                      'I2':'SMALLINT',
                                      'I' :'INTEGER',
                                      'I8':'BIGINT',
                                      'TS':'TIMESTAMP'}
TERADATA_BYTE_TYPES = ['BF','BV']
TERADATA_NUMBER_TYPES = ['D','F','I1','I2','I','I8']
TERADATA_CHAR_TYPES = ['CF','CV']
TERADATA_LOB_TYPES = ['BO','CO']
TERADATA_DATETIME_TYPES = ['AT','DA','TS','SZ']


TERADATA_DEFAULT_CHARACTER_SET = FLOAD_CHARSET_KANJISJIS_0S

TERADATA_READ_USER='DOC'   #This user can read all databases and write to DOC.


#####################
# Python Date Format#
#####################
PYTHON_DATE_FORMAT = {'YYYYMM':'%Y%m',
                      'YYYYMMDD':'%Y%m%d',
                      'YYYYMMDDHHMM':'%Y%m%d%H%M',
                      'YYYYMMDDHHMMSS':'%Y%m%d%H%M%S',
                      'YYYY-MM-DD':'%Y-%m-%d',
                      'YYYY/MM/DD':'%Y/%m/%d',
                      'DD/MM/YYYY':'%d/%m%Y',
                      'MM/DD/YYYY':'%m/%d/%Y'}

#########################
# Standard Files String #
#########################
STDIN_FILE  = 'stdin.out'
STDOUT_FILE = 'stdout.out'
STDERR_FILE = 'stderr.out'

LOCKFILE_DIR = 'LOCKFILE_DIRECTORY'
