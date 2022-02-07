import datetime
import ftp,sftp,snow,sql_server,oracle    
import s3   
from multiprocessing.pool import ThreadPool
import os

#start 4 worker processes
pool = ThreadPool(processes=4)

def main():
    # Select the Source and Sink 
    source=int(input("Select source type(select number):\n1.FTP\n2.SFTP\n3.Oracle\n4.SQL Server\n\n"))
    sink=int(input("Select sink type(select number):\n1.S3\n2.Snowflake\n\n"))
    #print(source,sink)

    if source==1 and sink==1:
        print("FTP and S3 selected")

        ftp_file_path=input("Enter FTP file path(folder/file.csv): \n").strip()
        s3_uri=input("Enter file S3 uri(s3://bucket/file.csv): \n").strip()

        print("\n-----------------------------------------------------------------------------------\n")
        # Thread to Get record, column count and dataframe from S3 and FTP
        async_result2 = pool.apply_async(s3.dataframe_s3,(s3_uri,)) 
        async_result3 = pool.apply_async(ftp.dataframe_ftp,(ftp_file_path,))   

        df_sink=async_result2.get()         # Get the dataframe from S3
        df_source=async_result3.get()       # Get the dataframe from FTP

    elif source==1 and sink==2:
        print("FTP and Snowflake selected")

        ftp_file_path=input("Enter FTP file path(folder/file.csv): \n").strip()
        db=input("Enter Snowflake Database Name: \n").strip()
        schm=input("Enter Snowflake Schema Name: \n").strip()
        tb=input("Enter Snowflake Table Name: \n").strip()

        print("\n-----------------------------------------------------------------------------------\n")
        # Thread to Get record, column count and dataframe from Snowflake and FTP
        async_result2 = pool.apply_async(snow.dataframe_snow,(db,schm,tb,)) 
        async_result3 = pool.apply_async(ftp.dataframe_ftp,(ftp_file_path,))   

        df_sink=async_result2.get()         # Get the dataframe from Snowflake
        df_source=async_result3.get()       # Get the dataframe from FTP

    elif source==2 and sink==1:
        print("SFTP and S3 selected")

        sftp_file_path=input("Enter sFTP file path(folder/file.csv): \n").strip()
        s3_uri=input("Enter file S3 uri(s3://bucket/file.csv): \n").strip()

        print("\n----------------------------------------------------------------------------------\n")
        # Thread to Get record, column count and dataframe from S3 and SFTP               
        async_result2 = pool.apply_async(s3.dataframe_s3,(s3_uri,)) 
        async_result3 = pool.apply_async(sftp.dataframe_sftp,(sftp_file_path,)) 

        df_sink=async_result2.get()         # Get the dataframe from S3
        df_source=async_result3.get()       # Get the dataframe from SFTP

    elif source==2 and sink==2:
        print("SFTP and Snowflake selected")

        sftp_file_path=input("Enter sFTP file path(folder/file.csv): \n").strip()
        db=input("Enter Snowflake Database Name: \n").strip()
        schm=input("Enter Snowflake Schema Name: \n").strip()
        tb=input("Enter Snowflake Table Name: \n").strip()

        print("\n-----------------------------------------------------------------------------------\n")
        # Thread to Get record, column count and dataframe from Snowflake and SFTP
        async_result2 = pool.apply_async(snow.dataframe_snow,(db,schm,tb,)) 
        async_result3 = pool.apply_async(sftp.dataframe_sftp,(sftp_file_path,))   

        df_sink=async_result2.get()         # Get the dataframe from Snowflake
        df_source=async_result3.get()       # Get the dataframe from SFTP

    elif source==3 and sink==1:
        print("Oracle and S3 selected\n")

        table=input("Enter Oracle Table name: \n").strip()         
        s3_uri=input("Enter file S3 uri(s3://bucket/file.csv): \n").strip()
        
        print("\n---------------------------------------------------------------------------------\n")
        # Thread to Get record, column count and dataframe from S3 and Oracle
        async_result2 = pool.apply_async(s3.dataframe_s3,(s3_uri,)) 
        async_result3 = pool.apply_async(oracle.dataframe_oracle,(table,))

        df_sink=async_result2.get()         # Get the dataframe from S3
        df_source=async_result3.get()       # Get the dataframe from Oracle

    elif source==3 and sink==2:
        print("Oracle and Snowflake selected\n")

        table=input("Enter Oracle Table name: \n").strip()
        db=input("Enter Snowflake Database Name: \n").strip()
        schm=input("Enter Snowflake Schema Name: \n").strip()
        tb=input("Enter Snowflake Table Name: \n").strip()

        print("\n---------------------------------------------------------------------------------\n")
        # Thread to Get record, column count and dataframe from Snowflake and Oracle
        async_result2 = pool.apply_async(snow.dataframe_snow,(db,schm,tb,)) 
        async_result3 = pool.apply_async(oracle.dataframe_oracle,(table,))

        df_sink=async_result2.get()
        df_source=async_result3.get()

    elif source==4 and sink==1:
        print("SQL Server and S3 selected")

        sdb=input("Enter SQL Server Database name: \n").strip() 
        stb=input("Enter SQL Server Table name: \n").strip()         
        s3_uri=input("Enter file S3 uri(s3://bucket/file.csv): \n").strip()

        print("\n---------------------------------------------------------------------------------\n")
        # Thread to Get record, column count and dataframe from S3 and SQL Server
        async_result2 = pool.apply_async(s3.dataframe_s3,(s3_uri,)) 
        async_result3 = pool.apply_async(oracle.dataframe_oracle,(sdb,stb,))

        df_sink=async_result2.get()         # Get the dataframe from S3
        df_source=async_result3.get()       # Get the dataframe from SQL SERVER

    elif source==4 and sink==2:
        print("SQL Server and Snowflake selected")

        sdb=input("Enter SQL Server Database name: \n").strip() 
        stb=input("Enter SQL Server Table name: \n").strip() 

        db=input("Enter Snowflake Database Name: \n").strip()
        schm=input("Enter Snowflake Schema Name: \n").strip()
        tb=input("Enter Snowflake Table Name: \n").strip()

        print("\n---------------------------------------------------------------------------------\n")
        # Thread to Get record, column count and dataframe from Snowflake and SQL Server
        async_result2 = pool.apply_async(snow.dataframe_snow,(db,schm,tb,)) 
        async_result3 = pool.apply_async(sql_server.dataframe_sql,(sdb,stb,))

        df_sink=async_result2.get()
        df_source=async_result3.get()
        
    else:
        print("Invalid input")
    # End of Threads and Now Source and Sink dataframes are ready to compare
    process(df_source,df_sink)


# Get unmatched records from Source and Sink & compare both dataframes
def process(source_df,sink_df):
    #print(source_df,sink_df)
    #compare - If Rows and columns are same but values are different then write the different data to csv
    #print("--------------------------- compare -----------------------------------")
    try:
        comp=source_df.compare(sink_df,align_axis=0).rename(index={'self': 'Source', 'other': 'Sink'}, level=-1) # Compare the dataframes
        print(comp)
        if comp.shape[0]>0:             # If there are any differences in dataframes
            with open('compared_data.csv', 'w', newline='') as f:     # Write the differences to csv file
                comp.to_csv(f)
                f.write("\n")
    except Exception as e:
        print(e)

    try:
        if sink_df.equals(source_df):            # Check if both dataframes are equal
            print("\nDataframe is Equal")
        elif source_df.columns.equals(sink_df.columns):

            #source_df.sort_values(by=list(source_df.columns)[:2],inplace=True)
            #sink_df.sort_values(by=list(sink_df.columns)[:2],inplace=True)
            print("---- Source Dataframe ----\n",source_df)
            print("---- Sink Dataframe ----\n",sink_df)
            # source_df['xid']=source_df.index
            # sink_df['xid']=sink_df.index
            # source_df=source_df.reset_index(drop=True)    # Reset index of ftp dataframe
            # sink_df=sink_df.reset_index(drop=True)      # Reset index of s3 dataframe

            source_df['count'] = source_df.groupby(list(source_df.columns)).cumcount()
            sink_df['count'] = sink_df.groupby(list(sink_df.columns)).cumcount()

            source_df['index']=source_df.index
            source_df = source_df.reset_index(drop=True)
            sink_df['index']=sink_df.index
            sink_df = sink_df.reset_index(drop=True)

            # Get the unmatched records from Source and Sink using outer join method
            # Note: It will compare based on count with all the fileds on dataframes to avoid duplicates
            un_df=source_df.merge(sink_df,how='outer',on=list(source_df.columns[:-1]),indicator=True)[lambda x: x['_merge']!='both']
            # print(un_df)
            # Rename the column value of _merge to Source or Sink insted of left_only or right_only
            un_df["_merge"].replace({"left_only": "Source", "right_only": "Sink"}, inplace=True)
            # Rename the column index_x to Source_index and index_y to Sink_index
            un_df.rename(columns = {'index_x':'Source_index','index_y':"Sink_index"}, inplace = True)
            # drop the count column
            un_df = un_df.drop(['count'], axis=1)
            print(un_df)
            if un_df.shape[0]>0:
                print("\nDataframe is Not Equal\n")
                with open('unmatched_data.csv', 'w',newline='') as f: # Write unmatched records to csv file
                    un_df.to_csv(f)
                    f.write("\n")

            else:
                print("\nDataframe is Equal but records are suffled....\n")
        else:
            print("\nDataframe have different columns.....")
    except Exception as e:
        print(e)

# Start of main function
if __name__ == "__main__":
    start = datetime.datetime.now().replace(microsecond=0)  # Start time
    print("starts at --",start,"\n")
    main()                                                  # main function
    end=datetime.datetime.now().replace(microsecond=0)      # End time
    print("\nends at - - ",end)
    print("Total Time took - - :",end-start)                # Total time took
    os.system("pause")

# End of Program