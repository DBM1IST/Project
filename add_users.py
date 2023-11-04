

import psycopg2
import pathlib

connection = None


file_dir = str(pathlib.Path(__file__).parent.absolute()) + '/Data/user.csv'

file = open(file_dir, "r")

first = 0;


try:
	# Creating connection
    connection = psycopg2.connect(host = 'localhost',
                                user = 'postgres',
                                port = 5432,
                                dbname = 'postgres')
    cursor = connection.cursor()
    
    for line in file:
        
        #The first line 
        if first == 0:
            first = -1
            continue
        work_line = line.split("\n")[0]
        work_line = work_line.split(",")
                
        #Making query
        sql_user = 'INSERT INTO user(username, name, age, n_post, followers) VALUES (%s, %s, %s, %s, %s);'
        data = (work_line[1],work_line[2], work_line[3], work_line[4], work_line[5]);
        
        # Feed the data to the SQL query as follows to avoid SQL injection
        cursor.execute(sql_user, data)
        
    	
    	# Commit the update (without this step the database will not change)
        connection.commit()
    
    

#If something goes wrong
except Exception as error_description:
	print(error_description)
	cursor.close()

finally:
	if connection is not None:
		connection.close()

file.close()
