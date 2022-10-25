#paths are needed to test local repositories and need to be set with sys

import juice_postgres_exec as jcr

#conn is dict  #conn like {'host':'url','database':'dbname','user':'uname','password':'pword'}
#conn = {'host':'url or name of server','port':'port if relevant, using it for docker sql server', ///
#database':'database name','user':'username','password':'[***]'}
conn = {'host':'hostname','port':'port','database':'dbname','user':'username','password':'password'}

#sql is dict  #structure like {'q0':'sql query','q1':'sql query'}

sql =     {
                    "q0":"drop schema dev cascade",
                    "q1":"create schema dev",
                    "q2":"select * from information_schema.tables",
                    "q3":"create table dev.tbl (val int,col varchar(10))",
                    "q4":"insert into dev.tbl values (1,'hi'), (2,'bye')",
                    "q5":"select * from dev.tbl"
                }

jcr.exec(conn,sql)