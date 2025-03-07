import pymssql
conn = pymssql.connect(
    server="mrvevo-dev-server.database.windows.net",
    database="MRVEvo-dev-db",
    user="mrverv_dev-admin",
    password="Whole35%"
)
cursor = conn.cursor()
cursor.execute("SELECT * FROM Items")
print(cursor.fetchall())
conn.close()