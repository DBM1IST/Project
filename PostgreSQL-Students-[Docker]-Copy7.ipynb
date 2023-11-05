{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [],
   "source": [
    "import psycopg2  #import of the psycopg2 python library\n",
    "import pandas as pd #import of the pandas python library\n",
    "import pandas.io.sql as psql\n",
    "import pathlib\n",
    "\n",
    "## No transaction is started when commands are executed and no commit() or rollback() is required. \n",
    "from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Connected Successfully to PostgreSQL server!!\n"
     ]
    }
   ],
   "source": [
    "try:\n",
    "    # Connect to the postgreSQL server with username, and password credentials\n",
    "    con = psycopg2.connect(user = \"postgres\",\n",
    "                                  password = \"postgres\",\n",
    "                                  host = \"postgres\",\n",
    "                                  port = \"5432\")\n",
    "    \n",
    "    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);\n",
    "    print(\"Connected Successfully to PostgreSQL server!!\")\n",
    "    \n",
    "    # Obtain a DB Cursor to perform database operations\n",
    "    cursor = con.cursor();\n",
    "except (Exception, psycopg2.Error) as error :\n",
    "     print (\"Error while connecting to PostgreSQL\", error)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Error While Creating the DB:  database \"socialnet\" already exists\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#DB_name variable    \n",
    "name_Database   = \"socialnet\";\n",
    "\n",
    "# Create DB statement\n",
    "sqlCreateDatabase = \"CREATE DATABASE \"+name_Database+\";\"\n",
    "\n",
    "try:\n",
    "    # Execute a SQL command: this creates a new DB\n",
    "    cursor.execute(sqlCreateDatabase);\n",
    "    print(\"Database '\"+name_Database+\"' Created Successfully!\")\n",
    "except (Exception, psycopg2.Error) as error :\n",
    "    print(\"Error While Creating the DB: \",error)\n",
    "    \n",
    "finally:\n",
    "    # Close communication with the database\n",
    "    cursor.close() #to close the cusrsor\n",
    "    con.close() #to close the connection/ we will open a new connection to the created DB"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<connection object at 0x7f3d60025e40; dsn: 'user=postgres password=xxx host=postgres port=5432', closed: 1>"
      ]
     },
     "execution_count": 4,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "con"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "connected again to the server and cusor now on socialnet DB !!\n"
     ]
    }
   ],
   "source": [
    "# get a new connection but this time point to the created \"socialnet\" DB.\n",
    "con = psycopg2.connect(user = \"postgres\",\n",
    "                       password = \"postgres\", \n",
    "                       host = \"postgres\", #Using Docker we can refer to containers by name\n",
    "                       port = \"5432\",\n",
    "                       database = \"socialnet\")\n",
    "\n",
    "try:\n",
    "    # Obtain a new DB Cursor (to \"socialnet\" DB )\n",
    "    cursor = con.cursor();\n",
    "    print(\"connected again to the server and cusor now on socialnet DB !!\")\n",
    "except (Exception, psycopg2.Error) as error:\n",
    "    print(\"Error in Connection\",error)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Error While Creating the DB:  relation \"users\" already exists\n",
      "\n"
     ]
    }
   ],
   "source": [
    "#Create Users Table\n",
    "\n",
    "try:\n",
    "    #table_name variable\n",
    "    usersTable=\"users\"\n",
    "    create_usersTablee_query = '''CREATE TABLE '''+ usersTable +''' \n",
    "              (username TEXT  PRIMARY KEY     NOT NULL,\n",
    "               name           TEXT    NOT NULL,\n",
    "               age        INT    NOT NULL,\n",
    "               n_post          INT NOT NULL,\n",
    "               followers          INT NOT NULL\n",
    "               ); '''\n",
    "\n",
    "    #Execute this command (SQL Query)\n",
    "    cursor.execute(create_usersTablee_query)\n",
    "    \n",
    "    # Make the changes to the database persistent\n",
    "    con.commit()\n",
    "    print(\"Table (\"+ usersTable +\") created successfully in PostgreSQL \")\n",
    "except (Exception, psycopg2.Error) as error:\n",
    "    # if it exits with an exception the transaction is rolled back.\n",
    "    con.rollback()\n",
    "    print(\"Error While Creating the DB: \",error)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "cursor = con.cursor()\n",
    "\n",
    "file_dir = str(pathlib.Path().absolute()) + '/data/user.csv'\n",
    "file = open(file_dir, \"r\")\n",
    "first = 0\n",
    "data_to_insert = []  # Lista para almacenar los datos a insertar\n",
    "\n",
    "for line in file:\n",
    "    if first == 0:\n",
    "        first = -1\n",
    "        continue\n",
    "    work_line = line.strip().split(\",\")  # Elimina el salto de línea y divide por comas\n",
    "    data_to_insert.append(work_line)  # Agrega los datos a la lista\n",
    "\n",
    "sql_insert_users = \"INSERT INTO users (username, name, age, n_post, followers) VALUES (%s, %s, %s, %s, %s)\"\n",
    "\n",
    "\n",
    "try:\n",
    "    # Ejecuta el INSERT statement para cada conjunto de datos\n",
    "    for user_data in data_to_insert:\n",
    "        data = (user_data[0], user_data[1],user_data[2],user_data[3], user_data[4])\n",
    "        cursor.execute(sql_insert_users, data)\n",
    "         \n",
    "\n",
    "    # Realiza la inserción en la base de datos\n",
    "    con.commit()\n",
    "    # El número de filas/tuplas insertadas\n",
    "    count = cursor.rowcount\n",
    "    print(count, \"Registros insertados exitosamente en la tabla users\")\n",
    "\n",
    "except (Exception, psycopg2.Error) as error:\n",
    "    con.rollback()\n",
    "    print(\"Error al insertar los datos en la tabla, Detalles:\", error)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "#use Pandas to print the result in tabular form\n",
    "# Don't RUN before you put your SQL Query\n",
    "my_table = pd.read_sql(\"SELECT username FROM users WHERE name LIKE 'J%' \", con)\n",
    "display(my_table)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "my_table.size\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.6"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
