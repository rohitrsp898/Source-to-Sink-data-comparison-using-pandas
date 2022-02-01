
import cx_Oracle
import config
import pandas as pd

# making dsn string
dsn_tns = cx_Oracle.makedsn(config.o_hostname, config.o_port, config.o_sid)
# Create connection with oracle database
connection = cx_Oracle.connect(config.o_username, config.o_password, dsn_tns)
# cur=connection.cursor()
# cnt=f'''SELECT count(*) FROM {table}'''
# cur.execute(cnt)
#print(cur.fetchall()[0][0])

def dataframe_oracle(table):
    #print("\n --------------------------- ORACLE SOURCE -----------------------------\n")
    print("ORACLE table: ",table)
    #print("\n")
    try:
        df1=pd.read_sql(f'''SELECT * FROM {table}''',connection)
        print(f"ORACLE '{table}' table Columns :",tuple(df1.columns))
        print(f"ORACLE '{table}' table Columns count :",df1.shape[1])
        print(f"ORACLE '{table}' table records count :",df1.shape[0],"\n")
        return df1
    except Exception as e:
        print(e)




