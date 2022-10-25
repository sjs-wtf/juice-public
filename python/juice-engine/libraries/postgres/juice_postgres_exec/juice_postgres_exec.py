import psycopg2 as rc
import datetime as dt
import pandas as pd

def msg(string):
    msgstr = '['+str(dt.datetime.now())+'] : '+string
    return msgstr

def dt14():
    return str(type(str(dt.datetime.now()))().join(filter(type(str(dt.datetime.now())).isdigit, str(dt.datetime.now())))[:14])
    
def exec(conn,sql):

    print(msg('starting validation.'))

    if not type(conn) is dict:
        raise TypeError(msg("Connection values must be passed as dict: ({'host':'','database':'','user':'','password':''}."))
    if not type(sql) is dict:
        raise TypeError(msg("sql must be passed as a dict: ({'q1':'query','q2':'query',etc}."))
    
    print(msg('starting script.'))

    cstr = "dbname='"+conn['database']+"' port='"+conn['port']+"' user='"+conn['user']+"' host='"+conn['host']+"' password='"+conn['password']+"'"

    print(msg('Connection argument is ' + cstr + '.'))
      
    connection = rc.connect(cstr)

    try:
        print(msg('connecting to ' + conn['host']))
        connection = rc.connect(cstr)
    except:
        Exception(msg('connection to ' + conn['host'] + ' failed.'))
    
    cursor: rc.cursor = connection.cursor()

    try:
        print(msg('action - executing sql scripts.'))
        for key, value in sql.items():
            print(msg('Executing query ' + str(key)+ ' - "' + str(value) + '".'))
            cursor.execute(value)
            print(msg('Query executed ' + str(key)+ ' - "' + str(value) + '".'))
            try:
                print(msg('Fetching dataframe head results of ' + str(key)+ ' - "' + str(value) + '".'))
                df = pd.DataFrame(cursor.fetchall())
                print(df.head(10))
            except:
                print(msg('No results for ' + str(key)+ ' - "' + str(value) + '".'))
                continue
            connection.commit()
    except:
        raise Exception(msg('action failed - executing sql scripts.'))

    print(msg('action passed - executing sql scripts.'))
    connection.commit()
    
    connection.close()
    print(msg('completed successfully!'))