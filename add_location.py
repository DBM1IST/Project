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
