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

	#Create Users Table

try:
    #table_name variable
    usersTable="users"
    create_usersTablee_query = '''CREATE TABLE '''+ usersTable +''' 
              (username TEXT  PRIMARY KEY     NOT NULL,
               name           TEXT    NOT NULL,
               age        INT    NOT NULL,
               n_post          INT NOT NULL,
               followers          INT NOT NULL
               ); '''

    #Execute this command (SQL Query)
    cursor.execute(create_usersTablee_query)
    
    # Make the changes to the database persistent
    con.commit()
    print("Table ("+ usersTable +") created successfully in PostgreSQL ")
except (Exception, psycopg2.Error) as error:
    # if it exits with an exception the transaction is rolled back.
    con.rollback()
    print("Error While Creating the DB: ",error)

cursor = con.cursor()

file_dir = str(pathlib.Path().absolute()) + '/data/user.csv'
file = open(file_dir, "r")
first = 0
data_to_insert = []  # Lista para almacenar los datos a insertar

for line in file:
    if first == 0:
        first = -1
        continue
    work_line = line.strip().split(",")  # Elimina el salto de línea y divide por comas
    data_to_insert.append(work_line)  # Agrega los datos a la lista

sql_insert_users = "INSERT INTO users (username, name, age, n_post, followers) VALUES (%s, %s, %s, %s, %s)"


try:
    # Ejecuta el INSERT statement para cada conjunto de datos
    for user_data in data_to_insert:
        data = (user_data[0], user_data[1],user_data[2],user_data[3], user_data[4])
        cursor.execute(sql_insert_users, data)
         

    # Realiza la inserción en la base de datos
    con.commit()
    # El número de filas/tuplas insertadas
    count = cursor.rowcount
    print(count, "Registros insertados exitosamente en la tabla users")

except (Exception, psycopg2.Error) as error:
    con.rollback()
    print("Error al insertar los datos en la tabla, Detalles:", error)

#use Pandas to print the result in tabular form
# Don't RUN before you put your SQL Query
my_table = pd.read_sql("SELECT * FROM users", con)
display(my_table)
