import pyodbc


def lambda_handler(event, context):

    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=server_name;'
                          'Database=db_name;'
                          'Trusted_Connection=yes;')

    cursor = conn.cursor()
    cursor.execute('SELECT * FROM db_name.Table')

    for row in cursor:
        print(row)

