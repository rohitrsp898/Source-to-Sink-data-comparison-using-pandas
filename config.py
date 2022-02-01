from configparser import ConfigParser

file='config.ini'
config = ConfigParser()
config.read(file)

service_name=config["s3"]["service_name"]
region_name=config["s3"]["region_name"]
aws_access_key_id=config["s3"]["aws_access_key_id"]
aws_secret_access_key=config["s3"]["aws_secret_access_key"]
   
ftp_host=config["ftp"]["hostname"]
ftp_port=config["ftp"]["port"]
ftp_username=config["ftp"]["username"]
ftp_password=config["ftp"]["password"]
    
sftp_host=config["sftp"]["hostname"]
sftp_port=config["sftp"]["port"]
sftp_username=config["sftp"]["username"]
sftp_password=config["sftp"]["password"]

o_hostname=config["oracle"]["hostname"] 
o_port=config["oracle"]["port"]
o_username=config["oracle"]["username"]
o_password=config["oracle"]["password"]
o_sid=config["oracle"]["sid"]
    
snow_host=config["snow"]["hostname"]
snow_port=config["snow"]["port"]
snow_username=config["snow"]["username"]
snow_password=config["snow"]["password"]
snow_account=config["snow"]["account"]
snow_warehouse=config["snow"]["warehouse"]
snow_database=config["snow"]["database"]
snow_schema=config["snow"]["schema"]
