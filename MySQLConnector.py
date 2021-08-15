import pandas as pd
import pymysql
import logging
import sshtunnel
from sshtunnel import SSHTunnelForwarder
import configparser




class MySQLConnector : 
    def __init__(self, session='MySQL', config_path='config.ini'): 
        self.config_parser = configparser.ConfigParser()
        self.config_parser.read(config_path)
        self.config = self.config_parser[session]
        self.connection = None
        self.tunnel = None
        self.ssh_host = self.config['ssh_host']
        self.ssh_username = self.config['ssh_username']
        self.ssh_password = self.config['ssh_password']
        self.database_username = self.config['database_username']
        self.database_password = self.config['database_password']
        self.database_name = self.config['database_name']
        self.localhost = self.config['localhost']
    
        
    def open_ssh_tunnel(self, verbose=False):
        """Open an SSH tunnel and connect using a username and password.
        
        :param verbose: Set to True to show logging
        :return tunnel: Global SSH tunnel connection
        """
        
        if verbose:
            sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG

        self.tunnel = SSHTunnelForwarder(
            (self.ssh_host, 22),
            ssh_username = self.ssh_username,
            ssh_password = self.ssh_password,
            remote_bind_address = ('127.0.0.1', 3306)
        )
        
        self.tunnel.start()

    def connect(self):
        """Connect to a MySQL server using the SSH tunnel connection
        
        :return connection: Global MySQL database connection
        """        
        self.connection = pymysql.connect(
            host='127.0.0.1',
            user=self.database_username,
            passwd=self.database_password,
            db=self.database_name,
            port=self.tunnel.local_bind_port
        )

    def run_query(self, sql):
        """Runs a given SQL query via the global database connection.
        
        :param sql: MySQL query
        :return: Pandas dataframe containing results
        """
        
        return pd.read_sql_query(sql, self.connection)
    
    def insert_dateframe(self, df, schema_name:str, table_name: str, pk_list : list) : 
        cursor = self.connection.cursor()
        df = df.where(pd.notnull(df), None)
        df = df.replace({np.nan: None})
        sql_statement = f"INSERT INTO {schema_name}.{table_name} ({','.join(df.columns.tolist())}) VALUES ({', '.join(['%s'] * len(df.columns))}) ON DUPLICATE KEY UPDATE  {', '.join([f'{col} = VALUES({col})' for col in df.columns  if col not in pk_list ])}"
        print(sql_statement)

        data = list(df.itertuples(index=False, name=None))


        cursor.executemany(sql_statement, data)
        self.connection.commit()



    def disconnect(self):
        """Closes the MySQL database connection.
        """
        
        self.connection.close()

    def close_ssh_tunnel(self):
        """Closes the SSH tunnel connection.
        """
        
        self.tunnel.close

if __name__ == "__main__":
    # open_ssh_tunnel()
    # mysql_connect()
    # df = run_query("SELECT * FROM Stock_Index")
    # print(df.head())
    # mysql_disconnect()
    # close_ssh_tunnel()
    pass


