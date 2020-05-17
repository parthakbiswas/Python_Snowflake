# PARTHA K BISWAS (RICKY)
# This progran loads a large csv file into the Snowflake Internal stage AFTER splitting the csv in to
# multiple smaller files to improve efficiency. After that they are copied into the Snowflake target Table using parallel
# processing inside the snowflake warehouse.
#======================================================================================================
# First import the threading module to support this functionality
#======================================================================================================
import threading
# Second import the Snowflake module
import snowflake.connector as sf

# Import Rest of the modules
import logging
import os
import sys

#Import Custom Large file splitter
import csv_splitter
#==============================================================================================

def log_file_setup(p_log_file_name=None):

    # This function sets up the log file

    file_name = p_log_file_name
    if file_name is None:
        file_name = "C:/Users/north/PycharmProjects/SnowflakeTest/venv/snowflake_python_thread.log"

    # -- (> ---------- SECTION=begin_logging -----------------------------
    logging.basicConfig(
        filename=file_name,
        level=logging.INFO)
    # -- <) ---------- END_SECTION ---------------------------------------
#====================================================================================================

def args_to_properties(args):

        """
        PURPOSE:
            Read the command-line arguments and store them in a dictionary.
            Command-line arguments should come in pairs, e.g.:
                "--user MyUser"
        INPUTS:
            The command line arguments (sys.argv).
        RETURNS:
            Returns the dictionary.
        DESIRABLE ENHANCEMENTS:
            Improve error detection and handling.
        """
        # python3 multithreadbulkload.py --warehouse COMPUTE_WH --database DEMO_DB --schema PUBLIC --account xxxx.us-east-1 --user JOHNDOEXX
        # --largefile file1
        # Putting parameters in a Dictionary
        connection_parameters = {}

        i = 1
        while i < len(args) - 1:
            property_name = args[i]
            # Strip off the leading "--" from the tag, e.g. from "--user".
            property_name = property_name[2:]
            property_value = args[i + 1]
            connection_parameters[property_name] = property_value
            i += 2

        return connection_parameters

        # -- <) ---------- END_SECTION ---------------------------------------
#===============================================================================================

def sfConnect(argv):
    # This script creates a function that establishes a connection to a Snowflake instance

    """
        PURPOSE:
            This connects gets account and login information from the
            environment variables and command-line parameters, connects to the
            server, and returns the connection object.
        INPUTS:
            argv: This is usually sys.argv, which contains the command-line
                  parameters. It could be an equivalent substitute if you get
                  the parameter information from another source.
        RETURNS:
            A connection.
    """

    # Retieve Password from Environmental Variable
    PASSWORD = os.getenv('SNOWSQL_PWD')

    # Get the other login info etc. from the command line.
    if len(argv) != 17:
        msg = "ERROR: Please pass the following command-line parameters:\n"
        msg += "--warehouse <warehouse> --database <db> --schema <schema> "
        msg += "--user <user> --account <account>  --largefile <largefile_to_split>"
        msg += "--stage <stage> --fileformat <fileformat>"
        print(msg)
        sys.exit(-1)
    else:
        connection_parameters = args_to_properties(argv)
        USER = connection_parameters["user"]
        ACCOUNT = connection_parameters["account"]
        WAREHOUSE = connection_parameters["warehouse"]
        DATABASE = connection_parameters["database"]
        SCHEMA = connection_parameters["schema"]
        LARGEFILE = connection_parameters["largefile"]

    # Optional: for internal testing only.
    try:
        PORT = connection_parameters["port"]
        PROTOCOL = connection_parameters["protocol"]
    except:
        PORT = ""
        PROTOCOL = ""

    print("Connecting...")
    if PROTOCOL is None or PROTOCOL == "" or PORT is None or PORT == "":
        # -- (> ------------------- SECTION=connect_to_snowflake ---------
        conn = sf.connect(
            user=USER,
            password=PASSWORD,
            account=ACCOUNT,
            warehouse=WAREHOUSE,
            database=DATABASE,
            schema=SCHEMA
        )
        # -- <) ---------------------------- END_SECTION -----------------
    else:
        conn = sf.connect(
            user=USER,
            password=PASSWORD,
            account=ACCOUNT,
            warehouse=WAREHOUSE,
            database=DATABASE,
            schema=SCHEMA,
            # Optional: for internal testing only.
            protocol=PROTOCOL,
            port=PORT
        )

    print('Connection validated')

    list_conn_wh=[conn,WAREHOUSE]

    return list_conn_wh

# -- <) ---------- END_SECTION --------------------------------------
# ==============================================================================================
# Define the threads class called sfExecutionThread.
# This class is an object which stores all the necessary details for the thread.
# Specifically, we include a threadID so that we can identify individual threads.
# When executed, each thread will announce that it is starting, execute sfExecuteInSnowflake(),
# then announce that it is exiting.
class sfExecutionThread (threading.Thread):
   def __init__(self, threadID, sqlQuery):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.sqlQuery = sqlQuery
   def run(self):
      print('Starting {0}: {1}'.format(self.threadID, self.sqlQuery))
      sfExecuteInSnowflake(self.sqlQuery)
      print('Exiting {0}: {1}'.format(self.threadID, self.sqlQuery))

# Define the function that will be executed within each thread
def sfExecuteInSnowflake (sfQuery):

    # Establish connection
    ## Make sure you insert the right login credentials below.
    list_conn_wh = sfConnect(sys.argv)
    sfConnection=list_conn_wh[0]
    sfWarehouse=list_conn_wh[1]

    # Use role defined in function input
    sfConnection.cursor().execute('USE ROLE ACCOUNTADMIN')
    # Use warehouse defined in function input
    sfConnection.cursor().execute('USE WAREHOUSE {0}'.format(sfWarehouse))

    # Increase the session timeout if desired
    sfConnection.cursor().execute('ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 86400')



    # Execute the query sfQuery in Snowflake
    sfConnection.cursor().execute(sfQuery)
#==================================================================================================

def clean_up(argv):

    # Get the password from an appropriate environment variable, if
    # available.
    PASSWORD = os.getenv('SNOWSQL_PWD')
    connection_parameters = args_to_properties(argv)
    USER = connection_parameters["user"]
    ACCOUNT = connection_parameters["account"]
    WAREHOUSE = connection_parameters["warehouse"]
    DATABASE = connection_parameters["database"]
    SCHEMA = connection_parameters["schema"]
    LARGEFILE = connection_parameters["largefile"]
    # Cleanup the stage and close connection

    conn = sf.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
    )
    # Use role defined in function input
    conn.cursor().execute('USE ROLE ACCOUNTADMIN')

    sql = "remove @DDB_STG01/customer pattern ='.*.csv.gz'"
    conn.cursor().execute(sql)
    conn.close()
# -- <) ===============================================================================
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# M A I N     F L O W
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
log_file_setup()
connection_parameters = args_to_properties(sys.argv)


# Split LARGE files
csv_splitter.split(connection_parameters['largefile'])

DATABASE = connection_parameters["database"]
SCHEMA = connection_parameters["schema"]
STAGE = connection_parameters["stage"]
FILEFORMAT = connection_parameters["fileformat"]
LARGEFILE = connection_parameters["largefile"]


#========================================================================================
# STEP 1 - Move split files to the Stage Location in Snowflake
# Define the list of variables which determine the data that will be loaded
#========================================================================================
splittedFIles=csv_file = 'C:/Users/north/OneDrive/Documents/Snowflake/SampleData/SplitFIleFdr/*.csv'
variablesList = [
    {
        'sourceLocation': f'{splittedFIles}',
        'destinationTable': f'{STAGE}/customer/'
    }]

# Define an empty list to populate with COPY INTO statements
PutStatements = []

# Loop through the members of variablesList and construct the PUT statements
# Use .format()  replace the {0} and {1} with variables destinationTable and sourceLocation
for member in variablesList:
  PutStatements.append(f"put file://{member['sourceLocation']}  {member['destinationTable']} auto_compress=true")
  #PutStatements.append(f"put file://{member['destinationTable']} {member['sourceTable']} FILE_FORMAT = (FORMAT_NAME = {FILEFORMAT});")

# Create the empty list of threads
threads = []

# Define a counter which will be used as the threadID
counter = 0

# Loop through each statement in the copyIntoStatements list,
# adding the sfExecutionThread thread to the list of threads
# and incrementing the counter by 1 each time.
for statement in PutStatements:
    threads.append(sfExecutionThread(counter, statement))
    counter += 1
# Execute the threads
for thread in threads:
    thread.start()

#==============================================================================================
# STEP 2 - Move taged files to Snowflake Tables
# Define the list of variables which determine the data that will be loaded
# =============================================================================================
variablesList = [
  {
    'sourceLocation': f'{STAGE}/customer',
    'destinationTable': f'{DATABASE}.{SCHEMA}.CUSTOMER_LARGE'
  },
  {
    'sourceLocation': f'{STAGE}/customer',
    'destinationTable': f'{DATABASE}.{SCHEMA}.CUSTOMER_LARGE'
  }]

# Define an empty list to populate with COPY INTO statements
copyIntoStatements = []

# Loop through the members of variablesList and construct the COPY INTO statements
# Use .format() to replace the {0} and {1} with variables destinationTable and sourceLocation
for member in variablesList:
  copyIntoStatements.append("COPY INTO {0} FROM {1} FILE_FORMAT = (FORMAT_NAME = {2});".format(member['destinationTable'], member['sourceLocation'],FILEFORMAT))

# Create the empty list of threads
threads = []

# Define a counter which will be used as the threadID
counter = 1

# Loop through each statement in the copyIntoStatements list,
# adding the sfExecutionThread thread to the list of threads
# and incrementing the counter by 1 each time.
for statement in copyIntoStatements:
    threads.append(sfExecutionThread(counter, statement))
    counter += 1
# Execute the threads
for thread in threads:
    thread.start()
#-----------------------------------------------------------------------------------------
clean_up(sys.argv)
#=========================================================================================
#                     E           N              D
#=========================================================================================
# OBSERVATION- As an experiment, there were 3 jobs fired in this program. They were each assigned a
# separate thread. You will see that even though they were fired 1 after the other, they did end in the
# same order since they were processed parallely. It is assumed that the tasks did not have dependancies.
# This process increases the performance drastically.
#
#------------------------------------------------------------------------------------------------
# Whilst Snowflake is designed for elastic warehouses that can be scaled up on demand, as
# data volumes increase, we often see extra-small (XS) warehouses are faster and cheaper overall,
# provided your data is prepared and your load is executed in the right way. When loading compressed
# files that are only 100MB, Snowflake’s largest and smallest warehouses will both load the file
# incredibly quickly. A common approach when loading large quantities of files is to use a larger
# warehouse as this supports more threads for the load (a.k.a. more files loading simultaneously).
# However, as the data volume increases, this will eventually result in queuing regardless of
# warehouse size if loading in a single session (one large SQL script full of COPY INTO commands).
#
# ========================================================================================