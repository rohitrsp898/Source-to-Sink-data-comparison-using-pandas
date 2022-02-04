
import config
import pandas as pd
#import snowflake.connector
import pypyodbc

#DS_SF is dsn 64 bit ODBC driver for Snowflake
connection = pypyodbc.connect(f'DSN=DS_SF;UID={config.snow_username};PWD={config.snow_password};')

def dataframe_snow(db,schm,tb):
    try:
        df1=pd.read_sql(f'''SELECT * from {db}.{schm}.{tb}''',connection)
        print("Snowflake table: ",tb)
        print(f"Snowflake '{tb}' table Columns :",tuple(df1.columns))
        print(f"Snowflake '{tb}' table Columns count :",df1.shape[1])
        print(f"Snowflake '{tb}' table records count :",df1.shape[0],"\n")
        return df1
    except Exception as e:
            print(e)

#.IDEA_QA_TEST_DB.A_IDEA_T_STG_FIN_CARD_TEMP_34
#print(dataframe_snow("IDEA_QA_TEST_DB","IDEA_QA_TEST_DB","A_IDEA_T_STG_FIN_CARD_TEMP_34"))
        

# Checking connection to Snowflake

#cursor=con.cursor()
# # Execute a statement that will generate a result set.
#sql = f"SELECT * from {db}.{schm}.{tb}"
#cursor.execute(sql)
# # Fetch the result set from the cursor and deliver it as the Pandas DataFrame.
#df = pd.read_sql(sql, connection)
#print("\n --------------------------- ORACLE SOURCE -----------------------------\n")
#print("Snowflake table: ",tb)
#print("\n")