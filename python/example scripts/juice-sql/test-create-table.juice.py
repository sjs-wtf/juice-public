#paths are needed to test local repositories and need to be set with sys

import juice_postgres_create as jcr

#conn is dict  #conn like {'host':'url','database':'dbname','user':'uname','password':'pword'}
#conn = {'host':'url or name of server','port':'port if relevant, using it for docker sql server', ///
#database':'database name','user':'username','password':'[***]'}
conn = {'host':'localhost','port':'49153','database':'dev','user':'postgres','password':'QWE123'}

#schema is str  #schema is table schema

schema = 'dev'

#table is str
#table is table name

table = 'table'

#drop_existing is int
#drop_existing = 0 is no drop, fail if table exists
#drop_existing = 1 is drop-create
#drop_existing = 2 is copy-drop-create, copy table to schema.table_timestamp eg: tablename_202209091222157

drop_existing = 2

#structure is dict  #structure like {'columnname':'data type','column2name':'column2datatype'}

structure =     {
                    'id':'integer not null',
                    'guid':'varchar(36) not null',
                    'email':'varchar(100)',
                    'created_on':'timestamp',
                    'modified_on':'timestamp',
                    'version':'integer',
                    'hash':'varchar(32)'
                }

jcr.create(conn,schema,table,structure,drop_existing)
