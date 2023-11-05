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
