import pypyodbc  #pip install pypyodbc
import config
import pandas as pd

# conn = pypyodbc.connect("DRIVER={SQL Server};"+f"SERVER={config.ss_host};UID={config.ss_username};PWD={config.ss_password};DATABASE={config.ss_database}")

# df=pd.read_sql("SELECT * from dbo.A_IDEA_T_STG_FIN_CARD_TEMP_34",conn)
# print(df)