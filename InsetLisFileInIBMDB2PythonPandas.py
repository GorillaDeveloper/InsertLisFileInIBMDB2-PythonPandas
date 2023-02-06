import os
import pandas as pd
import ibm_db

dsn_hostname = "Hostname"
dsn_uid = "user id"
dsn_pwd = "password"
dsn_driver = "{IBM DB2 ODBC DRIVER}"
dsn_database = "bludb"
dsn_port = "32731"
dsn_protocol = "TCPIP"
dsn_security = "SSL"

dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd,dsn_security)

# Connect to the database


# Set the path to the relative destination folder
path = 'data/SymbolsShortLongName'
# Get a list of all ILS files in the relative destination folder
files = [f for f in os.listdir(path) if f.endswith('.lis')]

# Create Connection String
conn = ibm_db.connect(dsn,"", "")
print(conn)
# Iterate through each file
print("companies data insertion start")
for file in files:
    file_path = os.path.join(path, file)
    file_path = file_path.replace("\\", '/' )
    
    with open(file_path, 'r') as original:
        first_line = original.readline()
        if first_line != "symbolic_name|short_name|long_name|":
            data = original.read()
            with open(file_path, 'w') as modified: modified.write("symbolic_name|short_name|long_name|\n" + data)
        
    df = pd.read_csv(file_path, delimiter="|")
    df = df.drop(df.columns[-1], axis=1)
    df.columns = ['symbolic_name','short_name','long_name']
    print(df)
    for index, row in df.iterrows():
        sql = "INSERT INTO COMPANY (symbolic_name, short_name, long_name) VALUES ('{}', '{}', '{}')".format(row['symbolic_name'], row['short_name'], row['long_name'])
        ibm_db.exec_immediate(conn,sql)
ibm_db.close(conn)
print("company names inserted successfully")