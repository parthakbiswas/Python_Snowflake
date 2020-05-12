import logging
import os
import sys
import pandas as pd



# -- (> ---------------------- SECTION=import_connectors ---------------------
import snowflake.connector
# from snowflake.connector import DictCursor
# -- <) ---------------------------- END_SECTION ----------------------------


class SnwClass:

    """
    PURPOSE:
        This is the sample code for programs that use the Snowflake
        Connector for Python.
        This class is intended primarily for:
            * Sample programs, e.g. in the documentation.
            * Tests.
    """


    def __init__(self, p_log_file_name = None):

        """
        PURPOSE:
            This does any required initialization steps, which in this class is
            basically just turning on logging.
        """

        file_name = p_log_file_name
        if file_name is None:
            file_name = "C:/Users/north/PycharmProjects/SnowflakeTest/venv/tmpsnowflake_python_onnector.log"

        # -- (> ---------- SECTION=begin_logging -----------------------------
        logging.basicConfig(
            filename=file_name,
            level=logging.INFO)
        # -- <) ---------- END_SECTION ---------------------------------------


    # -- (> ---------------------------- SECTION=main ------------------------
    def main(self, argv):

        """
        PURPOSE:
            Most tests follow the same basic pattern in this main() method:
               * Create a connection.
               * Set up, e.g. use (or create and use) the warehouse, database,
                 and schema.
               * Run the queries (or do the other tasks, e.g. load data).
               * Clean up. In this test/demo, we drop the warehouse, database,
                 and schema. In a customer scenario, you'd typically clean up
                 temporary tables, etc., but wouldn't drop your database.
               * Close the connection.
        """

        # Read the connection parameters (e.g. user ID) from the command line
        # and environment variables, then connect to Snowflake.
        connection = self.create_connection(argv)

        # Set up anything we need (e.g. a separate schema for the test/demo).
        self.set_up(connection)

        # Do the "real work", for example, create a table, insert rows, SELECT
        # from the table, etc.
        self.do_the_real_work(connection)

        # Clean up. In this case, we drop the temporary warehouse, database, and
        # schema.
        self.clean_up(connection)

        print("\nClosing connection...")
        # -- (> ------------------- SECTION=close_connection -----------------
        connection.close()
        # -- <) ---------------------------- END_SECTION ---------------------

    # -- <) ---------------------------- END_SECTION=main --------------------


    # -- <) ============================== START METHOD ==============================
    def args_to_properties(self, args):

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
    # python3 MYTEST.py --warehouse COMPUTE_WH --database DEMO_DB --schema PUBLIC --account xxxx.us-east-1 --user JOHNDOEXX
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

    # -- <) ============================== START METHOD ==============================
    def create_connection(self, argv):

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

        # Get account and login information from environment variables and
        # command-line parameters.
        # Note that ACCOUNT might require the region and cloud platform where
        # your account is located, in the form of
        #     '<your_account_name>.<region>.<cloud>'
        # for example
        #     'xy12345.us-east-1.azure')
        # -- (> ----------------------- SECTION=set_login_info ---------------

        # Get the password from an appropriate environment variable, if
        # available.
        PASSWORD = os.getenv('SNOWSQL_PWD')

        # Get the other login info etc. from the command line.
        if len(argv) < 11:
            msg = "ERROR: Please pass the following command-line parameters:\n"
            msg += "--warehouse <warehouse> --database <db> --schema <schema> "
            msg += "--user <user> --account <account> "
            print(msg)
            sys.exit(-1)
        else:
            connection_parameters = self.args_to_properties(argv)
            USER = connection_parameters["user"]
            ACCOUNT = connection_parameters["account"]
            WAREHOUSE = connection_parameters["warehouse"]
            DATABASE = connection_parameters["database"]
            SCHEMA = connection_parameters["schema"]
            # Optional: for internal testing only.
            try:
                PORT = connection_parameters["port"]
                PROTOCOL = connection_parameters["protocol"]
            except:
                PORT = ""
                PROTOCOL = ""

        # If the password is set by both command line and env var, the
        # command-line value takes precedence over (is written over) the
        # env var value.

        # If the password wasn't set either in the environment var or on
        # the command line...
        if PASSWORD is None or PASSWORD == '':
            print("ERROR: Set password, e.g. with SNOWSQL_PWD environment variable")
            sys.exit(-2)
        # -- <) ---------------------------- END_SECTION ---------------------

        # Optional diagnostic:
        #print("USER:", USER)
        #print("ACCOUNT:", ACCOUNT)
        #print("WAREHOUSE:", WAREHOUSE)
        #print("DATABASE:", DATABASE)
        #print("SCHEMA:", SCHEMA)
        #print("PASSWORD:", PASSWORD)
        #print("PROTOCOL:" "'" + PROTOCOL + "'")
        #print("PORT:" + "'" + PORT + "'")

        print("Connecting...")
        if PROTOCOL is None or PROTOCOL == "" or PORT is None or PORT == "":
            # -- (> ------------------- SECTION=connect_to_snowflake ---------
            conn = snowflake.connector.connect(
                user=USER,
                password=PASSWORD,
                account=ACCOUNT,
                warehouse=WAREHOUSE,
                database=DATABASE,
                schema=SCHEMA
                )
            # -- <) ---------------------------- END_SECTION -----------------
        else:
            conn = snowflake.connector.connect(
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

        return conn

    # -- <) ============================== START METHOD ==============================
    def set_up(self, connection):

        """
        PURPOSE:
            Create or Replace existing Tables in Snowflake DW
        """
        connection.cursor().execute("USE ROLE ACCOUNTADMIN")

        connection.cursor().execute(
            "CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.CUSTOMER ("
            "C_CUSTKEY NUMBER (38, 0)," 
            "C_NAME VARCHAR (25),"
            "C_ADDRESS VARCHAR (40)," 
            "C_NATIONKEY  NUMBER (38,0),"
            "C_PHONE VARCHAR (15)," 
            "C_ACCTBAL NUMBER (12,2),"
            "C_MKTSEGMENT VARCHAR (10)," 
            "C_COMMENT VARCHAR (117)"
            ");"
            )

        connection.cursor().execute(
            "CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.ORDERS ("
            "O_ORDERKEY NUMBER (38, 0),"  
            "O_CUSTKEY NUMBER (38, 0)," 
            "O_ORDERSTATUS VARCHAR (1),"
            "O_TOTALPRICE NUMBER (12,2)," 
            "O_ORDERDATE DATE,"
            "O_ORDERPRIORITY VARCHAR (15)," 
            "O_CLERK VARCHAR (15),"
            "O_SHIPPRORITY NUMBER (38,0)," 
            "O_COMMENT VARCHAR (79)"
            ")"
        )

        connection.cursor().execute(
            "CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.LINEITEM ("
            "L_ORDERKEY NUMBER (38, 0),"  
            "L_PARTKEY NUMBER (38, 0)," 
            "L_SUPPKEY NUMBER (38, 0)," 
            "L_LINENUMBER NUMBER (38, 0),"   
            "L_QUANTITY NUMBER (12,2),"
            "L_EXTENDEDPRICE NUMBER (12,2)," 
            "L_DISCOUNT NUMBER (12,2),"  
            "L_TAX NUMBER (12,2),"
            "L_RETURNFLAG VARCHAR (1),"  
            "L_LINESTATUS VARCHAR (1)," 
            "L_SHIPDATE DATE,"
            "L_COMMITDATE DATE,"
            "L_RECEIPTDATE DATE,"  
            "L_SHIPINSTRUCT VARCHAR (25)," 
            "L_SHIPMODE VARCHAR (10),"
            "L_COMMENT VARCHAR (44)"
            ")"
        )

        connection.cursor().execute(
            "CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.NATION ("
            "N_NATIONKEY NUMBER (38, 0),"  
            "N_NAME VARCHAR (25),"
            "N_REGIONKEY NUMBER (38,0)," 
            "N_COMMENT VARCHAR (152)"
            ")"
        )

        connection.cursor().execute(
            "CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.PART ("
            "P_PARTKEY NUMBER (38, 0),"  
            "P_NAME VARCHAR (55),"  
            "P_MFGR VARCHAR (25)," 
            "P_BRAND VARCHAR (10),"
            "P_TYPE VARCHAR (25)," 
            "P_SIZE NUMBER (38,0),"
            "P_CONTAINER VARCHAR (10)," 
            "P_RETAILPRICE NUMBER (12,2)," 
            "P_COMMENT VARCHAR (23)"
            ")"

        )

        connection.cursor().execute(
            "CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.SUPPLIER ("
            "S_SUPPKEY NUMBER (38, 0),"  
            "S_NAME VARCHAR (25),"  
            "S_ADDRESS VARCHAR (40)," 
            "S_NATIONKEY NUMBER (38,0),"
            "S_PHONE VARCHAR (15)," 
            "S_ACCTBAL NUMBER (12,2),"
            "S_COMMENT VARCHAR (101)"
            ")"
        )

        connection.cursor().execute(
            "CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.PARTSUPP ("
            "PS_PARTKEY NUMBER (38, 0),"  
            "PS_SUPPKEY NUMBER (38,0),"  
            "PS_AVAILQTY NUMBER (38,0)," 
            "PS_SUPPLYCOST NUMBER (12,2),"
            "PS_COMMENT VARCHAR (199)"
            ")"
        )
        """
                PURPOSE:
                    Copy csv files to the SNOWFLAKE STAGE
         """

        csv_file = 'C:/Users/north/OneDrive/Documents/Snowflake/SampleData/Customer.csv'
        sql = "put file://{0} @DDB_STG01 auto_compress=true".format(csv_file)
        connection.cursor().execute(sql)
        sql1 = "COPY INTO CUSTOMER FROM @DDB_STG01/Customer.csv.gz FILE_FORMAT=(format_name = DDB_FFT01)"
        connection.cursor().execute(sql1)

        csv_file = 'C:/Users/north/OneDrive/Documents/Snowflake/SampleData/Orders.csv'
        sql = "put file://{0} @DDB_STG01 auto_compress=true".format(csv_file)
        connection.cursor().execute(sql)
        sql1 = "COPY INTO ORDERS FROM @DDB_STG01/Orders.csv.gz FILE_FORMAT=(format_name = DDB_FFT01)"
        connection.cursor().execute(sql1)

        csv_file = 'C:/Users/north/OneDrive/Documents/Snowflake/SampleData/LineItem.csv'
        sql = "put file://{0} @DDB_STG01 auto_compress=true".format(csv_file)
        connection.cursor().execute(sql)
        sql1 = "COPY INTO LINEITEM FROM @DDB_STG01/LineItem.csv.gz FILE_FORMAT=(format_name = DDB_FFT01)"
        connection.cursor().execute(sql1)

        csv_file = 'C:/Users/north/OneDrive/Documents/Snowflake/SampleData/Nation.csv'
        sql = "put file://{0} @DDB_STG01 auto_compress=true".format(csv_file)
        connection.cursor().execute(sql)
        sql1 = "COPY INTO NATION FROM @DDB_STG01/Nation.csv.gz FILE_FORMAT=(format_name = DDB_FFT01)"
        connection.cursor().execute(sql1)

        csv_file = 'C:/Users/north/OneDrive/Documents/Snowflake/SampleData/Part.csv'
        sql = "put file://{0} @DDB_STG01 auto_compress=true".format(csv_file)
        connection.cursor().execute(sql)
        sql1 = "COPY INTO PART FROM @DDB_STG01/Part.csv.gz FILE_FORMAT=(format_name = DDB_FFT01)"
        connection.cursor().execute(sql1)

        csv_file = 'C:/Users/north/OneDrive/Documents/Snowflake/SampleData/Supplier.csv'
        sql = "put file://{0} @DDB_STG01 auto_compress=true".format(csv_file)
        connection.cursor().execute(sql)
        sql1 = "COPY INTO SUPPLIER FROM @DDB_STG01/Supplier.csv.gz FILE_FORMAT=(format_name = DDB_FFT01)"
        connection.cursor().execute(sql1)

        csv_file = 'C:/Users/north/OneDrive/Documents/Snowflake/SampleData/PartSupp.csv'
        sql = "put file://{0} @DDB_STG01 auto_compress=true".format(csv_file)
        connection.cursor().execute(sql)
        sql1 = "COPY INTO PARTSUPP FROM @DDB_STG01/PartSupp.csv.gz FILE_FORMAT=(format_name = DDB_FFT01)"
        connection.cursor().execute(sql1)


        connection.cursor().close()

    # -- <) ============================== START METHOD ==============================
    def do_the_real_work(self, conn):



        # Create a cursor for this connection.
        cursor1 = conn.cursor()

        # This is an example of an SQL statement we might want to run.
        c1 = "CREATE OR REPLACE VIEW DEMO_DB.PUBLIC.V_DISTRIBUTION AS SELECT O.O_ORDERKEY as Order_Nbr  , O.O_TOTALPRICE "
        c2 = "as Total_Price, O_ORDERDATE as Order_Date,O_CLERK as Order_Agent,O.O_ORDERSTATUS AS "
        c3 = "Order_Status,L.L_QUANTITY as Order_Quantity, L.L_EXTENDEDPRICE as Extended_Price, "
        c4 = "L.L_SHIPDATE as Ship_Date,L.L_RECEIPTDATE as Receipt_Date, L.L_SHIPMODE as Ship_Mode, "
        c5 = "P.P_NAME AS Part_Name, NC.N_NAME AS Cust_Country,NS.N_NAME AS Supp_Country, "
        c6 = "S.S_Name as Supplier_Name, PS.PS_AVAILQTY as Available_Qty,PS.PS_SUPPLYCOST as Cost_of_part "
        c7 = "FROM DEMO_DB.PUBLIC.ORDERS O JOIN DEMO_DB.PUBLIC.LINEITEM L ON O.O_ORDERKEY = L.L_ORDERKEY "
        c8 = "JOIN DEMO_DB.PUBLIC.PART P ON L.L_PARTKEY = P.P_PARTKEY "
        c9 = "JOIN DEMO_DB.PUBLIC.SUPPLIER S ON L.L_SUPPKEY = S.S_SUPPKEY "
        c10 = "JOIN DEMO_DB.PUBLIC.CUSTOMER C ON O.O_CUSTKEY = C.C_CUSTKEY "
        c11 = "JOIN DEMO_DB.PUBLIC.NATION NC ON C.C_NATIONKEY = NC.N_NATIONKEY "
        c12 = "JOIN DEMO_DB.PUBLIC.NATION NS ON S.S_NATIONKEY = NS.N_NATIONKEY "
        c13 = "JOIN DEMO_DB.PUBLIC.PARTSUPP PS ON P.P_PARTKEY = PS.PS_PARTKEY AND S.S_SUPPKEY = PS_SUPPKEY"
        command = c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13

        # Execute SQL
        cursor1.execute(command)


        # Get the results (should be only one):
        #for row in cursor1:
        #    print(row[0])
        # Close this cursor.
        cursor1.close()

        # Read data into dataframe from V_DISTRIBUTION
        cur = conn.cursor()
        try:
            sql = "SELECT * FROM V_DISTRIBUTION"
            results = cur.execute(sql).fetchall()
            column_names = [col[0] for col in cur.description]
            df = pd.DataFrame(results, columns=column_names)
        finally:
            cur.close()

    # -- <) ============================== START METHOD ==============================


    #-----------------------------------------------------------------------------------------
    def clean_up(self, connection):

        # Cleanup the stage and close connection

       sql2 = "remove @DDB_STG01 pattern ='.*.csv.gz'"
       connection.cursor().execute(sql2)

       sql3 = "SELECT * FROM CUSTOMER"
       self.fetch_pandas_old(connection,sql3)

    #--------------------------------------------------------------------------

    # -- <) ============================== START METHODS ==============================

    def fetch_pandas_old(self, conn, sql):

        # Create a cursor for this connection.
        cur = conn.cursor()
        cur.execute(sql)

        rows = 0
        while True:
            dat = cur.fetchmany(200)
            if not dat:
               break
            df = pd.DataFrame(dat, columns=cur.description)
            df1 = pd.DataFrame(dat)
            rows += df.shape[0]
            print(df1.loc[0])
        #print(rows)
        cur.close()

        # -- <) ---------------------------- END_SECTION ---------------------
# -- <) ==============================**** END ALL METHOD ****==============================

# ----------------------------------------------------------------------------

if __name__ == '__main__':
    pvb = SnwClass()
    pvb.main(sys.argv)