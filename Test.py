
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

sink_df=s3.dataframe_s3("s3://ideakafkatest/idea-cdf-ftp-20220120145144453/ftp/facts/INVENTORIES.csv")

source_df=oracle.dataframe_oracle("INVENTORIES")

# source_df1=source_df.reset_index(drop=True)    # Reset index of ftp dataframe
# sink_df1=sink_df.reset_index(inplace=True,drop=True)        # Reset index of oracle dataframe
# sink_df=sink_df.reset_index(drop=True)  
print(source_df)
print(sink_df)



un_df=source_df.merge(sink_df,how='outer',on=list(source_df.columns),indicator=True)[lambda x: x['_merge']!='both']
un_df["_merge"].replace({"left_only": "Source", "right_only": "Sink"}, inplace=True)

print(un_df)
#-----------------------

