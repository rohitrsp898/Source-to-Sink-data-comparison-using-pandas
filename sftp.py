import config
import pandas as pd
from smart_open import smart_open


def dataframe_sftp(sftp_file_path):
    #print("\n --------------------------- FTP SOURCE -----------------------------\n")
    print("sFTP file path: ",sftp_file_path+"\n")
    #print("\n")
    try:
        
        path=f"sftp://{config.sftp_username}:{config.sftp_password}@{config.sftp_host}:{config.sftp_port}/{sftp_file_path}"
        #print("sFTP file path: ---->",path)
        df1=pd.read_csv(smart_open(path))
        print("sFtp file Columns :",tuple(df1.columns))
        print("sFtp file Columns count :",df1.shape[1])
        print("sFtp file records count :",df1.shape[0],"\n")
        return df1
    except Exception as e:
        print(e)


