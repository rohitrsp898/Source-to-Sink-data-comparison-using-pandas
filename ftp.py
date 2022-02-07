
import config
import pandas as pd

# Create Dataframe from FTP object, Count the number of rows, columns and get columns names
def dataframe_ftp(ftp_file_path):
    #print("\n --------------------------- FTP SOURCE -----------------------------\n")
    print("FTP file path: ",ftp_file_path+"\n")
    #print("\n")
    try:
        path=f"ftp://{config.ftp_username}:{config.ftp_password}@{config.ftp_host}:{config.ftp_port}/{ftp_file_path}"
        df1=pd.read_csv(path)
        print("Ftp file Columns :",tuple(df1.columns))
        print("Ftp file Columns count :",df1.shape[1])
        print("Ftp file records count :",df1.shape[0],"\n")
        return df1
    except Exception as e:
        print(e)

# df=dataframe_ftp("/test.csv")
# print(df)
