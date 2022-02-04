import pypyodbc  #pip install pypyodbc
import config
import pandas as pd

#conn = pypyodbc.connect("DRIVER={SQL Server};"+f"SERVER={config.ss_host};UID={config.ss_username};PWD={config.ss_password};DATABASE={config.ss_database}")
# def dataframe_sql(table):
#     #print("\n --------------------------- SQL Server SOURCE -----------------------------\n")
#     print("SQL Server table: ",table)
#     #print("\n")
#     try:
#         df1=pd.read_sql(f'''SELECT * FROM {table}''',conn)
#         print(f"SQL Server '{table}' table Columns :",tuple(df1.columns))
#         print(f"SQL Server '{table}' table Columns count :",df1.shape[1])
#         print(f"SQL Server '{table}' table records count :",df1.shape[0],"\n")
#         return df1
#     except Exception as e:
#         print(e)
#df=dataframe_sql("dbo.tbl_test")
# df=pd.read_sql("SELECT * from dbo.A_IDEA_T_STG_FIN_CARD_TEMP_34",conn)
#print(df)

