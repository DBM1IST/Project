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
my_table = pd.read_sql("SELECT username FROM users WHERE name LIKE 'J%' ", con)
display(my_table)

sql = f'DROP TABLE IF EXISTS {channelsTable}'

# Ejecutar la sentencia SQL
cursor.execute(sql)

# Confirmar la eliminación
con.commit()

#Create Channel  Table

try:
    #table_name variable
    channelsTable="channels"
    create_channelsTablee_query = '''CREATE TABLE '''+ channelsTable +''' 
              (
              name_channels TEXT PRIMARY KEY NOT NULL,
              username TEXT NOT NULL,
              n_users INT NOT NULL
               ); '''

    #Execute this command (SQL Query)
    cursor.execute(create_channelsTablee_query)
    
    # Make the changes to the database persistent
    con.commit()
    print("Table ("+ channelsTable +") created successfully in PostgreSQL ")
except (Exception, psycopg2.Error) as error:
    # if it exits with an exception the transaction is rolled back.
    con.rollback()
    print("Error While Creating the DB: ",error)

my_table.size

cursor = con.cursor()

file_dir = str(pathlib.Path().absolute()) + '/data/channel.csv'
file = open(file_dir, "r")
first = 0
data_to_insert = []  # Lista para almacenar los datos a insertar

for line in file:
    if first == 0:
        first = -1
        continue
    work_line = line.strip().split(",")  # Elimina el salto de línea y divide por comas
    data_to_insert.append(work_line)  # Agrega los datos a la lista

sql_insert_channels = "INSERT INTO channels (name_channels,  username, n_users) VALUES (%s, %s, %s)"

try:
    # Ejecuta el INSERT statement para cada conjunto de datos
    for user_data in data_to_insert:
        data = (user_data[0], user_data[1], user_data[2])
        cursor.execute(sql_insert_channels, data)
         

    # Realiza la inserción en la base de datos
    con.commit()
    # El número de filas/tuplas insertadas
    count = cursor.rowcount
    print(count, "Registros insertados exitosamente en la tabla channel")

except (Exception, psycopg2.Error) as error:
    con.rollback()
    print("Error al insertar los datos en la tabla, Detalles:", error)

#use Pandas to print the result in tabular form
# Don't RUN before you put your SQL Query
my_table = pd.read_sql("SELECT * FROM channels", con)
display(my_table)


sql = f'DROP TABLE IF EXISTS {companyTable}'

# Ejecutar la sentencia SQL
cursor.execute(sql)

# Confirmar la eliminación
con.commit()


#Create Company Table

try:
    #table_name variable
    companyTable="company"
    create_companyTablee_query = '''CREATE TABLE '''+ companyTable +''' 
              (id_company TEXT PRIMARY KEY NOT NULL,
                deposit DECIMAL NOT NULL
               ); '''

    #Execute this command (SQL Query)
    cursor.execute(create_companyTablee_query)
    
    # Make the changes to the database persistent
    con.commit()
    print("Table ("+ companyTable +") created successfully in PostgreSQL ")
except (Exception, psycopg2.Error) as error:
    # if it exits with an exception the transaction is rolled back.
    con.rollback()
    print("Error While Creating the DB: ",error)


cursor = con.cursor()

file_dir = str(pathlib.Path().absolute()) + '/data/company.csv'
file = open(file_dir, "r")
first = 0
data_to_insert = []  # Lista para almacenar los datos a insertar

for line in file:
    if first == 0:
        first = -1
        continue
    work_line = line.strip().split(",")  # Elimina el salto de línea y divide por comas
    data_to_insert.append(work_line)  # Agrega los datos a la lista

sql_insert_company = "INSERT INTO company (id_company, deposit) VALUES (%s, %s)"

try:
    # Ejecuta el INSERT statement para cada conjunto de datos
    for user_data in data_to_insert:
        data = (user_data[0], user_data[1])
        cursor.execute(sql_insert_company, data)
         

    # Realiza la inserción en la base de datos
    con.commit()
    # El número de filas/tuplas insertadas
    count = cursor.rowcount
    print(count, "Registros insertados exitosamente en la tabla company")

except (Exception, psycopg2.Error) as error:
    con.rollback()
    print("Error al insertar los datos en la tabla, Detalles:", error)

#use Pandas to print the result in tabular form
# Don't RUN before you put your SQL Query
my_table = pd.read_sql("SELECT * FROM company", con)
display(my_table)



#Create Location Table

try:
    #table_name variable
    locationTable="location"
    create_locationTablee_query = '''CREATE TABLE '''+ locationTable +''' 
              (city TEXT  PRIMARY KEY     NOT NULL,
               country           TEXT    NOT NULL,
               interactions      INT NOT NULL
               ); '''

    #Execute this command (SQL Query)
    cursor.execute(create_locationTablee_query)
    
    # Make the changes to the database persistent
    con.commit()
    print("Table ("+ locationTable +") created successfully in PostgreSQL ")
except (Exception, psycopg2.Error) as error:
    # if it exits with an exception the transaction is rolled back.
    con.rollback()
    print("Error While Creating the DB: ",error)

cursor = con.cursor()

file_dir = str(pathlib.Path().absolute()) + '/data/location.csv'
file = open(file_dir, "r")
first = 0
data_to_insert = []  # Lista para almacenar los datos a insertar

for line in file:
    if first == 0:
        first = -1
        continue
    work_line = line.strip().split(",")  # Elimina el salto de línea y divide por comas
    data_to_insert.append(work_line)  # Agrega los datos a la lista

sql_insert_location = "INSERT INTO location (city, country, interactions) VALUES (%s, %s, %s)"

try:
    # Ejecuta el INSERT statement para cada conjunto de datos
    for user_data in data_to_insert:
        data = (user_data[0], user_data[1],user_data[2])
        cursor.execute(sql_insert_location, data)
         

    # Realiza la inserción en la base de datos
    con.commit()
    # El número de filas/tuplas insertadas
    count = cursor.rowcount
    print(count, "Registros insertados exitosamente en la tabla location")

except (Exception, psycopg2.Error) as error:
    con.rollback()
    print("Error al insertar los datos en la tabla, Detalles:", error)

#use Pandas to print the result in tabular form
# Don't RUN before you put your SQL Query
my_table = pd.read_sql("SELECT * FROM location", con)
display(my_table)

sql = f'DROP TABLE IF EXISTS {influencersTable}'

# Ejecutar la sentencia SQL
cursor.execute(sql)

# Confirmar la eliminación
con.commit()

try:
    #table_name variable
    influencersTable="influencers"
    create_influencersTablee_query = '''CREATE TABLE '''+ influencersTable +''' 
              ( id INT PRIMARY KEY NOT NULL,
              username TEXT REFERENCES users(username) 
              ); '''

    #Execute this command (SQL Query)
    cursor.execute(create_influencersTablee_query)
    
    # Make the changes to the database persistent
    con.commit()
    print("Table ("+ influencersTable +") created successfully in PostgreSQL ")
except (Exception, psycopg2.Error) as error:
    # if it exits with an exception the transaction is rolled back.
    con.rollback()
    print("Error While Creating the DB: ",error)

cursor = con.cursor()

file_dir = str(pathlib.Path().absolute()) + '/data/influencers.csv'
file = open(file_dir, "r")
first = 0
data_to_insert = []  # Lista para almacenar los datos a insertar

for line in file:
    if first == 0:
        first = -1
        continue
    work_line = line.strip().split(",")  # Elimina el salto de línea y divide por comas
    data_to_insert.append(work_line)  # Agrega los datos a la lista

sql_insert_influencers = "INSERT INTO influencers (id,username) VALUES (%s,%s)"


try:
    # Ejecuta el INSERT statement para cada conjunto de datos
    for user_data in data_to_insert:
        data = (user_data[0],user_data[1])
        cursor.execute(sql_insert_influencers, data)
         

    # Realiza la inserción en la base de datos
    con.commit()
    # El número de filas/tuplas insertadas
    count = cursor.rowcount
    print(count, "Registros insertados exitosamente en la tabla influencers")


except (Exception, psycopg2.Error) as error:
    con.rollback()
    print("Error al insertar los datos en la tabla, Detalles:", error)

#use Pandas to print the result in tabular form
# Don't RUN before you put your SQL Query
my_table = pd.read_sql("SELECT * FROM influencers", con)
display(my_table)

sql = f'DROP TABLE IF EXISTS {postTable}'

# Ejecutar la sentencia SQL
cursor.execute(sql)

# Confirmar la eliminación
con.commit()

#Create POST Table

try:
    #table_name variable
    postTable="post"
    create_postTablee_query = '''CREATE TABLE '''+postTable +''' 
              (id_post INT PRIMARY KEY NOT NULL,
                n_comments INT NOT NULL,
                n_like INT NOT NULL,
                type TEXT NOT NULL,
                username TEXT NOT NULL,
                id_company TEXT REFERENCES company(id_company),
                city TEXT REFERENCES location(city)
               ); '''

    #Execute this command (SQL Query)
    cursor.execute(create_postTablee_query)
    
    # Make the changes to the database persistent
    con.commit()
    print("Table ("+ postTable +") created successfully in PostgreSQL ")
except (Exception, psycopg2.Error) as error:
    # if it exits with an exception the transaction is rolled back.
    con.rollback()
    print("Error While Creating the DB: ",error)

cursor = con.cursor()

file_dir = str(pathlib.Path().absolute()) + '/data/post.csv'
file = open(file_dir, "r")
first = 0
data_to_insert = []  # Lista para almacenar los datos a insertar

for line in file:
    if first == 0:
        first = -1
        continue
    work_line = line.strip().split(",")  # Elimina el salto de línea y divide por comas
    data_to_insert.append(work_line)  # Agrega los datos a la lista

sql_insert_post = "INSERT INTO post (id_post, n_comments, n_like, type, username,id_company,city) VALUES (%s, %s, %s, %s, %s,%s,%s)"

try:
    # Ejecuta el INSERT statement para cada conjunto de datos
    for user_data in data_to_insert:
        data = (user_data[0], user_data[1],user_data[2],user_data[3], user_data[4],user_data[5],user_data[6])
        cursor.execute(sql_insert_post, data)
         

    # Realiza la inserción en la base de datos
    con.commit()
    # El número de filas/tuplas insertadas
    count = cursor.rowcount
    print(count, "Registros insertados exitosamente en la tabla post")

except (Exception, psycopg2.Error) as error:
    con.rollback()
    print("Error al insertar los datos en la tabla, Detalles:", error)

#use Pandas to print the result in tabular form
# Don't RUN before you put your SQL Query
my_table = pd.read_sql("SELECT * FROM post ", con)
display(my_table)


my_table = pd.read_sql("SELECT * FROM post WHERE id_company = 'Snaptags'", con)
display(my_table)

#Relevants posts that have been published by users who are not influencers
my_table = pd.read_sql("SELECT post.* FROM post INNER JOIN users ON post.username = users.username WHERE users.followers < 80000 ORDER BY post.n_like DESC ", con)
display(my_table)
#σ followers < 80000 (post ⨝ username = username (users))

#Relevants posts that have been published by users who are not influencers
my_table = pd.read_sql("SELECT post.* FROM post LEFT JOIN influencers ON post.username = influencers.username WHERE influencers.username IS NULL ORDER BY post.n_like DESC", con)
display(my_table)
#π post.* (post ⨝ username = username (influencers))

#- Number of relevant posts from each advertising company in ascending order > 27
my_table = pd.read_sql("SELECT DISTINCT id_company , COUNT () FROM post GROUP BY id_company HAVING count()>25 ORDER BY count DESC", con)
display(my_table)


#-	Locations with more than 5M interactions and with more than 5 relevant posts
my_table = pd.read_sql("SELECT location.* FROM location JOIN (SELECT city, COUNT() AS post_count FROM post GROUP BY city HAVING COUNT() > 5) post ON location.city = post.city GROUP BY location.city HAVING SUM(location.interactions) > 8000000 ORDER BY location.interactions DESC",con)
display(my_table)
#π location.* (σ SUM(interactions) > 8000000 (location ⨝ city = city ((π city, COUNT(*) (σ COUNT(*) > 5 (ρ city (post)))))))



# Post with more than 300,000 likes and more than 10,000 comments, Games type and located in Lyon. Limited
my_table = pd.read_sql("SELECT * FROM post WHERE n_like>300000 AND n_comments>10000 AND city='Lyon' AND type='Games' ORDER BY n_comments ASC OFFSET 0 LIMIT 5",con)
display(my_table)
# π * (σ n_like > 300000 ∧ n_comments > 10000 ∧ city = 'Lyon' ∧ type = 'Games' (σ OFFSET 0 LIMIT 5 (ρ n_comments (n_comments ASC (post)))))



# Influencers with more than 1000,000 followers, whose broadcast channel has less than 30,000 users and who do not have any relevant posts
my_table = pd.read_sql("SELECT users.*, post.id_post, channels.name_channels, channels.n_users FROM influencers LEFT JOIN users ON influencers.username = users.username LEFT JOIN post ON influencers.username = post.username LEFT JOIN channels ON influencers.username = channels.username WHERE users.followers > 1000000 AND post.username IS NULL AND channels.n_users >30000",con)
display(my_table)

# TOP 10 post with more likes and comments by users with 24 years or less
my_table = pd.read_sql("SELECT p.username , u.age, u.followers, p.id_post, p.n_like, p.n_comments, p.type, p.city FROM users u INNER JOIN post p ON u.username = p.username  WHERE u.age < 25 ORDER BY p.n_like DESC OFFSET 0 LIMIT 10 ;",con)
display(my_table)


