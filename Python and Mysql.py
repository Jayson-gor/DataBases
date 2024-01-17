#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import mysql.connector
from mysql.connector import Error


# In[3]:


def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host = host_name,
            user = user_name,
            psswd = user_password
        )
        print("Mysql database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
    return connection


pw = "Comfortzone"
db = "testdb"
connection = create_server_connection("localhost", "root", pw)


# In[4]:


import mysql.connector
from mysql.connector import Error

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password
        )
        print("MySQL database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
    return connection

pw = "Comfortzone"
db = "testdb"
connection = create_server_connection("localhost", "root", pw)


# In[6]:


import mysql.connector
from mysql.connector import Error

def create_server_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
    return connection

def list_tables(connection):
    cursor = connection.cursor()
    try:
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        return tables
    except Error as err:
        print(f"Error: '{err}'")

pw = "Comfortzone"
db = "testdb"
connection = create_server_connection("localhost", "root", pw, db)

# Select the database
cursor = connection.cursor()
cursor.execute(f"USE {db}")

# List tables
tables_list = list_tables(connection)
print(tables_list)


# In[9]:


# creating jayson database
def create_database (connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("DB created successfully")
    except Error as err:
        print(f"Error: '{err}'")
        
create_database_query = "Create database Jayson"
create_database(connection, create_database_query)


# In[11]:


# Executing sql queries
def execute_query (connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query was successful")
    except Error as err:
        print(f"Error: '{err}'")


# In[18]:


#creating tables
create_details_table = """
create table details(
kenyan_id int primary key,
Fname varchar(30) not null,
Mname varchar(30) not null,
Lname varchar(30) not null,
D_O_B date,
location varchar(20) not null);
"""

connection = create_server_connection("localhost", "root", pw, db)
execute_query(connection, create_details_table)

import mysql.connector
from mysql.connector import Error

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password
        )
        print("MySQL database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
    return connection

pw = "Comfortzone"
db = "testdb"
connection = create_server_connection("localhost", "root", pw)
# In[24]:


import mysql.connector
from mysql.connector import Error

def create_server_connection(host_name, user_name, user_password, db_name = None):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database = db_name
        )
        print("MySQL database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
    return connection

pw = "Comfortzone"
db = "jayson"
connection = create_server_connection("localhost", "root", pw)


# In[25]:


#creating tables
create_details_table = """
create table details(
kenyan_id int primary key,
Fname varchar(30) not null,
Mname varchar(30) not null,
Lname varchar(30) not null,
D_O_B date,
location varchar(20) not null);
"""

connection = create_server_connection("localhost", "root", pw, db)
execute_query(connection, create_details_table)


# In[42]:


#adding data
data_details = """
insert into details values
(34914069, "Jayson", "Ouko", "Gor", '1998-01-09', "Nairobi"),
(30884315, "Barry", "Osewe", "Senior", '1996-01-09', "Nairobi"),
(31994569, "Edwin", "Okun", "Ochieng", '1995-03-09', "Nairobi"),
(32914369, "Joannes", "Omondi", "Ochieng", '1998-01-06', "Nairobi");
"""

connection = create_server_connection("localhost", "root", pw, db)
execute_query(connection, data_details)


# In[41]:


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()  # Commit changes to the database
        print("Query executed successfully.")
    except Error as err:
        connection.rollback()  # Rollback changes in case of an error
        print(f"Error: {err}")
    finally:
        cursor.close()



# In[43]:


def read_query (connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")
        
        


# In[45]:


#using the select statement
q1 = """
select * from details;
"""

connection = create_server_connection("localhost", "root", pw, db)

results = read_query(connection, q1)
for result in results:
    print(result)


# In[46]:


#selecting few columns
q2 = """
select kenyan_id, Fname, Mname, D_O_B from details;
"""

connection = create_server_connection("localhost", "root", pw, db)

results = read_query(connection, q2)
for result in results:
    print(result)


# In[47]:


#selecting few columns
q3 = """
select distinct year(D_O_B) from details;
"""

connection = create_server_connection("localhost", "root", pw, db)

results = read_query(connection, q3)
for result in results:
    print(result)


# In[51]:


#using the select statement
q1 = """
select * from details;
"""

connection = create_server_connection("localhost", "root", pw, db)

results = read_query(connection, q1)
for result in results:
    print(result)


from_db = []

for result in results:
    result = list(result)
    from_db.append(result)
    
columns = ["kenyan_id", "Fname","Mname","Lname","D_O_B","location"]
df = pd.DataFrame(from_db, columns = columns)
df


# In[ ]:




