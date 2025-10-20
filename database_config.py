

from configparser import RawConfigParser

cfg=RawConfigParser()
cfg.read(r"C:\\develpment\\configure\\configuration.ini")
credential_cfg=dict(cfg.items("Database_voc"))

# SQL Server数据库配置
SQL_SERVER_CONFIG = {
    'server': credential_cfg['servername'],  
    'database': credential_cfg['database'],  
    'username': credential_cfg['username'],  
    'password': credential_cfg['password'], 
    'driver': 'ODBC Driver 17 for SQL Server'  # SQL Server ODBC驱动
}

def get_sql_server_connection_string():
    """获取SQL Server连接字符串"""
    config = SQL_SERVER_CONFIG
    return f'mssql+pyodbc://{config["username"]}:{config["password"]}@{config["server"]}/{config["database"]}?driver=ODBC+Driver+17+for+SQL+Server' 