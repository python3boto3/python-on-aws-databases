import pymysql


def lambda_handler(event, context):

    # connect to aurora db

    db = pymysql.connect('d1.c.us-east-2.rds.amazonaws.com', 'admin', '5678')
    cursor = db.cursor()
    cursor.execute("select version()")
    data = cursor.fetchone()
    data

    # Create a table
    sql = '''drop database kgptalkie'''
    cursor.execute(sql)

    sql = '''create database kgptalkie'''
    cursor.execute(sql)

    cursor.connection.commit()
    sql = '''use kgptalkie'''
    cursor.execute(sql)

    sql = '''
    create table person (
    id int not null auto_increment,
    fname text,
    lname text,
    primary key (id)
    )
    '''
    cursor.execute(sql)

    sql = '''show tables'''
    cursor.execute(sql)
    cursor.fetchall()
    (('person',),)
    sql = '''
    insert into person(fname, lname) values('%s', '%s')''' % ('laxmi', 'kant')
    cursor.execute(sql)
    db.commit()
    sql = '''select * from person'''
    cursor.execute(sql)
    cursor.fetchall()
    ((1, 'laxmi', 'kant'), (2, 'laxmi', 'kant'))
    sql = '''desc person'''
    cursor.execute(sql)
    cursor.fetchall()
    (('id', 'int(11)', 'NO', 'PRI', None, 'auto_increment'),
        ('fname', 'text', 'YES', '', None, ''),
        ('lname', 'text', 'YES', '', None, ''))
