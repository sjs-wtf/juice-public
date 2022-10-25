import psycopg2 as rc
import datetime as dt

def msg(string):
    msgstr = '['+str(dt.datetime.now())+'] : '+string
    return msgstr

def dt14():
    return str(type(str(dt.datetime.now()))().join(filter(type(str(dt.datetime.now())).isdigit, str(dt.datetime.now())))[:14])
    
def create(conn,schema,table,structure,drop_existing):

    print(msg('starting validation.'))

    if not type(conn) is dict:
        raise TypeError(msg("Connection values must be passed as dict: ({'host':'','database':'','user':'','password':''}."))
    if not type(schema) is str:
        raise TypeError(msg("Schema name must be passed as a string."))
    if not type(table) is str:
        raise TypeError(msg("Table name must be passed as a string."))
    if not type(structure) is dict:
        raise TypeError(msg("Structure must be passed as a dict: ({'name':'dataype','name':'datatype',etc}."))
    
    print(msg('starting script.'))

    cstr = "dbname='"+conn['database']+"' port='"+conn['port']+"' user='"+conn['user']+"' host='"+conn['host']+"' password='"+conn['password']+"'"

    print(msg('Connection argument is ' + cstr + '.'))
    
    tbl_name = schema + '.' + table

    expand_dict = ''

    for key, value in structure.items():
        expand_dict += (key + ' ' + value + ',')

    expand_dict = expand_dict.rstrip(expand_dict[-1])

    if drop_existing==1:
        sql_dtie = 'DROP TABLE IF EXISTS ' + tbl_name
    elif drop_existing==2:
        n_tbl_name = tbl_name + '_' + dt14()
        sql_dtie = 'DROP TABLE IF EXISTS ' + tbl_name
        sql_bck1 = 'CREATE TABLE ' + n_tbl_name + ' ( LIKE '+tbl_name+')'
        sql_bck2 = 'INSERT INTO ' + n_tbl_name + ' SELECT * FROM ' + tbl_name
    elif drop_existing==0:
        sql_dtie = ''
    
    sql_ct = "CREATE TABLE " + tbl_name + " (" + expand_dict + ")"
    connection = rc.connect(cstr)
    try:
        print(msg('connecting to ' + conn['host']))
        connection = rc.connect(cstr)
    except:
        Exception(msg('connection to ' + conn['host'] + ' failed.'))
    
    cursor: rc.cursor = connection.cursor()

    if drop_existing==1:
        try:
            print(msg('action - dropping table ' + tbl_name + '.'))
            cursor.execute(sql_dtie)
        except:
            raise Exception(msg('action failed - drop table - ' + tbl_name + '.'))
    print(msg('action passed - drop table ' + tbl_name + '.'))
    connection.commit()

    if drop_existing==2:
        try:
            print(msg('action - copying table ' + tbl_name + ' to ' + n_tbl_name + '.'))
            cursor.execute(sql_bck1)
            cursor.execute(sql_bck2)
            connection.commit()
            print(msg('action passed - copy table ' + tbl_name + ' to ' + n_tbl_name + '.'))
            print(msg('action - dropping table ' + tbl_name + '.'))
            cursor.execute(sql_dtie)
        except:
            raise Exception(msg('action failed - copy and drop drop table - ' + tbl_name + '.'))
    print(msg('action passed - copy and drop table ' + tbl_name + '.'))
    connection.commit()

    try:
        print(msg('action - create table - ' + tbl_name + '.'))
        cursor.execute(sql_ct)
    except:
        raise Exception(msg('action failed - create table - ' + tbl_name + '.'))
    print(msg('action passed - create table - ' + tbl_name + '.'))
    connection.commit()
    
    connection.close()
    print(msg('completed successfully!'))