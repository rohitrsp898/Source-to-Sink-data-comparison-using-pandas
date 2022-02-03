
import pandas as pd
from collections import Counter
import oracle,s3
from smart_open import smart_open
import config,sftp
# df1=pd.read_csv("INVENTORIES-test.csv")
# df2=pd.read_csv("INVENTORIES.csv")
# df1 = df1.reset_index()
# df2 = df2.reset_index()

# def diff_df_2(df1,df2):
#     diff111=pd.concat([df1, df2]).drop_duplicates(keep=False)
#     print(diff111)


# diff_df_2(df1,df2)



# print(df1)
# print(df2)

# diff=df1.merge(df2,how='outer',on=list(df1.columns),indicator=True)[lambda x: x['_merge']!='both']
# print(diff)

# diff["_merge"].replace({"left_only": "Source", "right_only": "Sink"}, inplace=True)
# #diff['_merge'][diff._merge.str.contains('left_only')] = 'Source'
# # diff_=diff._merge.str.replace(r'(^.*left_only.*$)', 'Source')#diff.loc[diff['_merge'].str.contains('left_only'), '_merge'] = 'FTP'
# # diff1=diff_._merge.str.replace(r'(^.*right_only.*$)', 'Sink')
# print(diff)



# # def diff_df(df1,df2):
# #     df = pd.concat([df1, df2])
# #     df = df.reset_index(drop=True)
# #     print(df)

# #     df_gpby = df.groupby(list(df.columns))

# #     print(df_gpby)

# #     idx = [x[0] for x in df_gpby.groups.values() if len(x) == 1]

# #     df=df.reindex(idx)

# #     print(df)
# #diff_df(df1,df2)

#sftp_df1=pd.read_csv(smart_open("sftp://sftpuser1:pass123@20.102.72.241:22/sftpuser1/demo-data/facts/INVENTORIES.csv"))
                                #sftp://sftpuser1:pass123@20.102.72.241:22/sftpuser1/demo-data/facts/INVENTORIES.csv
# sftp_df=sftp.dataframe_sftp("sftpuser1/demo-data/facts/INVENTORIES.csv")
# print(sftp_df)
#print(sftp_df1)


#---------------------Oracle______________________

# sink_df=s3.dataframe_s3("s3://ideakafkatest/idea-cdf-ftp-20220120145144453/ftp/facts/INVENTORIES.csv")

# source_df=oracle.dataframe_oracle("INVENTORIES")

# source_df1=source_df.reset_index(drop=True)    # Reset index of ftp dataframe
# sink_df1=sink_df.reset_index(inplace=True,drop=True)        # Reset index of oracle dataframe
# sink_df=sink_df.reset_index(drop=True)  
# print(source_df)
# print(sink_df)



# un_df=source_df.merge(sink_df,how='outer',on=list(source_df.columns),indicator=True)[lambda x: x['_merge']!='both']
# un_df["_merge"].replace({"left_only": "Source", "right_only": "Sink"}, inplace=True)

# print(un_df)
#-----------------------



f1="C:/Users/roprajap/Downloads/file1.csv"
f2="C:/Users/roprajap/Downloads/file2.csv"

# sink_df=pd.read_csv(f1)
# source_df=pd.read_csv(f2)
df_1=pd.read_csv(f1)
df_2=pd.read_csv(f2)
print(list(df_1.columns))


df_1['count'] = df_1.groupby(['id', 'name', 'skill', 'test']).cumcount()
df_2['count'] = df_2.groupby(['id', 'name', 'skill', 'test']).cumcount()

df_1['index']=df_1.index
df_1 = df_1.reset_index(drop=True)
df_2['index']=df_2.index
df_2 = df_2.reset_index(drop=True)

# df_tot = pd.concat([df_1,df_2], ignore_index=False)
print(df_1,df_2)

df_tot=df_1.merge(df_2,how='outer',on=list(df_1.columns[:-1]),indicator=True)[lambda x: x['_merge']!='both']
print(df_tot)
df_tot["_merge"].replace({"left_only": "Source", "right_only": "Sink"}, inplace=True)
df_tot.rename(columns = {'index_x':'Source_index','index_y':"Sink_index"}, inplace = True)
#df_tot = df_tot.drop_duplicates()
df_tot = df_tot.drop(['count'], axis=1)

print(df_tot)



# print(df)
# df.sort_values(by=df.columns[0],inplace=True)
# df['index']=df.index
# df = df.reset_index(drop=True)
# print(df)
# print(df.columns)


# try:
#     if sink_df.equals(source_df):            # Check if both dataframes are equal
#         print("Dataframe is Equal")
#     else:
#         print("Dataframe is Not Equal\n")
#         if source_df.columns.equals(sink_df.columns):

#             source_df.sort_values(by=source_df.columns[0],inplace=True)
#             sink_df.sort_values(by=sink_df.columns[0],inplace=True)
#             source_df['xid']=source_df.index
#             sink_df['xid']=sink_df.index
#             source_df=source_df.reset_index(drop=True)    # Reset index of ftp dataframe
#             sink_df=sink_df.reset_index(drop=True)      # Reset index of s3 dataframe
#             print("Source Dataframe \n",source_df)
#             print("Sink Dataframe \n",sink_df)
#             # Get the unmatched records from FTP and S3 using outer join method
#             # Note: It will compare based on index with all the fileds on dataframes to avoid duplicates
#             un_df=source_df.merge(sink_df,how='outer',on=list(source_df.columns[:-1]),indicator=True)[lambda x: x['_merge']!='both']
#             # print(un_df)
#             #un_df=pd.concat([source_df, sink_df]).drop_duplicates(keep=False)
#             # Rename the column value of _merge to Source or Sink insted of left_only or right_only
#             #un_df["_merge"].replace({"left_only": "Source", "right_only": "Sink"}, inplace=True)

#             print(un_df)
#             # if un_df.shape[0]==0:
#             #     print(" Dataframe have different columns.....")
#             with open('unmatched_data.csv', 'w',newline='') as f: # Write unmatched records to csv file
#                 un_df.to_csv(f)
#                 f.write("\n")
        
#         else:
#             print("Dataframe have different columns.....")
# except Exception as e:
#     print(e)
