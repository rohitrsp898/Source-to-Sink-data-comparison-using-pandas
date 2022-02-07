# Source-to-Sink-data-validation-using-pandas

In this, we are comparing source and sink data. If there are any mismatches of data then we will store those data as CSV files.

- Source - FTP, sFTP, Oracle and SQL Server.
- Sink - AWS S3 and Snowflake.

Functionality-
1. Find Unmatched records and store those records into compare.csv.
2. It also find suffled data, if we have same no of records but data are suffled it will store those data into unmatched.csv

Requirments-
1. Tools - Python 3.8 (64 bit) or higher, Excel or any text editor tool to view the csv file.
2. Packages - pypyodbc/pyodbc, pandas, boto3, smart_open, paramiko, cx_Oracle.
3. Additional odbc application - Oracle instant client light 64 bit, Snowflake 64 bit, SQL SERVER odbc driver 64 bit.

Configure ODBC Data source 64 bit for Snowflake- https://docs.informatica.com/data-integration/powerexchange-adapters-for-powercenter/10-5/powerexchange-for-snowflake-user-guide-for-powercenter/snowflake-pushdown-optimization/configuring-the-snowflake-odbc-driver/configuring-the-snowflake-odbc-driver-on-windows.html
