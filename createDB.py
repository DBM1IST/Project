import psycopg2  #import of the psycopg2 python library
import pandas as pd #import of the pandas python library
import pandas.io.sql as psql
import pathlib

## No transaction is started when commands are executed and no commit() or rollback() is required. 
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

try:
    # Connect to the postgreSQL server with username, and password credentials
    con = psycopg2.connect(user = "postgres",
                                  password = "postgres",
                                  host = "postgres",
                                  port = "5432")
    
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
    print("Connected Successfully to PostgreSQL server!!")
    
    # Obtain a DB Cursor to perform database operations
    cursor = con.cursor();
except (Exception, psycopg2.Error) as error :
     print ("Error while connecting to PostgreSQL", error)

#DB_name variable    
name_Database   = "socialnet";

# Create DB statement
sqlCreateDatabase = "CREATE DATABASE "+name_Database+";"

try:
    # Execute a SQL command: this creates a new DB
    cursor.execute(sqlCreateDatabase);
    print("Database '"+name_Database+"' Created Successfully!")
except (Exception, psycopg2.Error) as error :
    print("Error While Creating the DB: ",error)
    
finally:
    # Close communication with the database
    cursor.close() #to close the cusrsor
    con.close() #to close the connection/ we will open a new connection to the created DB

con

# get a new connection but this time point to the created "socialnet" DB.
con = psycopg2.connect(user = "postgres",
                       password = "postgres", 
                       host = "postgres", #Using Docker we can refer to containers by name
                       port = "5432",
                       database = "socialnet")

try:
    # Obtain a new DB Cursor (to "socialnet" DB )
    cursor = con.cursor();
    print("connected again to the server and cusor now on socialnet DB !!")
except (Exception, psycopg2.Error) as error:
    print("Error in Connection",error)
