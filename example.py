from MySQLConnector import MySQLConnector

mysql_connector  = MySQLConnector()

mysql_connector.open_ssh_tunnel()
mysql_connector.connect()

#hkex_securities_data = get_hkex_securities_list()
#print(hkex_securities_data)
#mysql_connector.insert_dateframe(hkex_securities_data, schema_name = 'securities_data', table_name = 'Hong_Kong', pk_list = ['ticker'])
print(mysql_connector.run_query("SELECT * FROM securities_data.Hong_Kong;"))

mysql_connector.disconnect()
mysql_connector.close_ssh_tunnel()