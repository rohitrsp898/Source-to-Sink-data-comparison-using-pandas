import datetime
import ftp,sftp,oracle,snow,sql_server        
import s3   
import pandas as pd
from multiprocessing.pool import ThreadPool
import os


pool = ThreadPool(processes=4)
 
# Get unmatched records from FTP and S3 and compare both dataframes
def process(source_df,sink_df):
    #print(source_df,sink_df)
    try:
        if sink_df.equals(source_df):            # Check if both dataframes are equal
            print("Dataframe is Equal")
        else:
            print("Dataframe is Not Equal\n")
            source_df=source_df.reset_index(drop=True)    # Reset index of ftp dataframe
            sink_df=sink_df.reset_index(drop=True)      # Reset index of s3 dataframe
            print("Source Dataframe \n",source_df)
            print("Sink Dataframe \n",sink_df)
            # Get the unmatched records from FTP and S3 using outer join method
            # Note: It will compare based on index with all the fileds on dataframes to avoid duplicates
            un_df=source_df.merge(sink_df,how='outer',on=list(source_df.columns),indicator=True)[lambda x: x['_merge']!='both']
            # print(un_df)

            # Rename the column value of _merge to Source or Sink insted of left_only or right_only
            un_df["_merge"].replace({"left_only": "Source", "right_only": "Sink"}, inplace=True)

            print(un_df)
            if un_df.shape[0]==0:
                print(" Dataframe have different columns.....")
            with open('unmatched_data.csv', 'w',newline='') as f: # Write unmatched records to csv file
                un_df.to_csv(f)
                f.write("\n")
    except Exception as e:
        print(e)

    #compare - If Rows and columns are same but values are different then write the different data to csv
    #print("--------------------------- compare -----------------------------------")
    try:
        comp=source_df.compare(sink_df,align_axis=0).rename(index={'self': 'Source', 'other': 'Sink'}, level=-1) # Compare the dataframes
        print(comp)
        if comp.shape[0]>0:             # If there are any differences in dataframes
            with open('Compare.csv', 'w', newline='') as f:     # Write the differences to csv file
                comp.to_csv(f)
                f.write("\n")
    except Exception as e:
        print(e)


if __name__ == "__main__":
    
    start = datetime.datetime.now().replace(microsecond=0)  # Start time
    print("starts at --",start,"\n")

    source=int(input("Select source type(select number):\n1.FTP\n2.SFTP\n3.Oracle\n4.SQL Server\n\n"))
    sink=int(input("Select sink type(select number):\n1.S3\n2.Snowflake\n\n"))
    print(source,sink)

    if source==1 and sink==1:
        print("FTP and S3 selected")
        ftp_file_path=input("Enter FTP file path(folder/file.csv): \n\n").strip()
        s3_uri=input("Enter file S3 uri(s3://bucket/file.csv): \n\n").strip()

        print("\n-----------------------------------------------------------------------------------\n")
        # Thread to Get record, column count and dataframe from S3
        async_result2 = pool.apply_async(s3.dataframe_s3,(s3_uri,)) 
        async_result3 = pool.apply_async(ftp.dataframe_ftp,(ftp_file_path,))   

        df_sink=async_result2.get()
        df_source=async_result3.get()

    elif source==1 and sink==2:
        print("FTP and Snowflake selected")
        ftp_file_path=input("Enter FTP file path(folder/file.csv): \n\n").strip()
        db=input("Enter Snowflake Database Name: \n").strip()
        schm=input("Enter Snowflake Schema Name: \n").strip()
        tb=input("Enter Snowflake Table Name: \n\n").strip()

        print("\n-----------------------------------------------------------------------------------\n")
        # Thread to Get record, column count and dataframe from S3
        async_result2 = pool.apply_async(snow.dataframe_snow,(db,schm,tb,)) 
        async_result3 = pool.apply_async(ftp.dataframe_ftp,(ftp_file_path,))   

        df_sink=async_result2.get()
        df_source=async_result3.get()

    elif source==2 and sink==1:
        print("SFTP and S3 selected")

        sftp_file_path=input("Enter sFTP file path(folder/file.csv): \n\n").strip()
        s3_uri=input("Enter file S3 uri(s3://bucket/file.csv): \n\n").strip()

        print("\n----------------------------------------------------------------------------------\n")
        # Thread to Get record, column count and dataframe from S3                
        async_result2 = pool.apply_async(s3.dataframe_s3,(s3_uri,)) 
        async_result3 = pool.apply_async(sftp.dataframe_sftp,(sftp_file_path,)) 

        df_sink=async_result2.get()
        df_source=async_result3.get()

    elif source==2 and sink==2:
        print("SFTP and Snowflake selected")
        sftp_file_path=input("Enter sFTP file path(folder/file.csv): \n\n").strip()
        db=input("Enter Snowflake Database Name: \n").strip()
        schm=input("Enter Snowflake Schema Name: \n").strip()
        tb=input("Enter Snowflake Table Name: \n\n").strip()

        print("\n-----------------------------------------------------------------------------------\n")
        
        async_result2 = pool.apply_async(snow.dataframe_snow,(db,schm,tb,)) 
        async_result3 = pool.apply_async(sftp.dataframe_sftp,(sftp_file_path,))   

        df_sink=async_result2.get()
        df_source=async_result3.get()

    elif source==3 and sink==1:
        print("Oracle and S3 selected\n")

        table=input("Enter Oracle Table name: \n").strip()
        s3_uri=input("Enter file S3 uri(s3://bucket/file.csv): \n").strip()
        
        print("\n---------------------------------------------------------------------------------\n")
        # Thread to Get record, column count and dataframe from S3
        async_result2 = pool.apply_async(s3.dataframe_s3,(s3_uri,)) 
        async_result3 = pool.apply_async(oracle.dataframe_oracle,(table,))

        df_sink=async_result2.get()
        df_source=async_result3.get()

    elif source==3 and sink==2:
        print("Oracle and Snowflake selected\n")

        table=input("Enter Oracle Table name: \n").strip()
        db=input("Enter Snowflake Database Name: \n").strip()
        schm=input("Enter Snowflake Schema Name: \n").strip()
        tb=input("Enter Snowflake Table Name: \n").strip()

        async_result2 = pool.apply_async(snow.dataframe_snow,(db,schm,tb,)) 
        async_result3 = pool.apply_async(oracle.dataframe_oracle,(table,))

        df_sink=async_result2.get()
        df_source=async_result3.get()

    elif source==4 and sink==1:
        print("SQL Server and S3 selected")



    elif source==4 and sink==2:
        print("SQL Server and Snowflake selected")
    else:
        print("Invalid input")

    #new
    process(df_source,df_sink)


    

    end=datetime.datetime.now().replace(microsecond=0)      # End time
    print("\nends at - - ",end)
    print("Total Time took - - :",end-start)            # Total time took

    os.system("pause")