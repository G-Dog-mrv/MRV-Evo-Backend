import pyodbc
conn = pyodbc.connect(
    "DRIVER=/usr/local/lib/libmsodbcsql.17.dylib;"
    "SERVER=mrvevo-dev-server.database.windows.net;"
    "DATABASE=MRVEvo-dev-db;"
    "UID=mrverv_dev-admin;"
    "PWD=Whole35%;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=60"
)
cursor = conn.cursor()
cursor.execute("SELECT * FROM Items")
print(cursor.fetchall())
conn.close()