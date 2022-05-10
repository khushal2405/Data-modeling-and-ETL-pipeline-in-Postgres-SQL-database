#!/usr/bin/env python
# coding: utf-8

# In[1]:


import psycopg2


# In[ ]:


from sql_queries import create_table_queries, drop_table_queries


# In[11]:


#conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=***********")


# In[12]:


#conn.set_session(autocommit=True)


# In[13]:


#cur = conn.cursor()


# In[ ]:


#cur.execute("DROP DATABASE IF EXISTS sparkifydb")


# In[18]:


#cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")


# In[19]:


#conn.close()


# In[16]:


def create_database():
    #establisshing cursor and connection with any default database
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=************")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    #before deleting or dropping anydata base make sure that there are no other session connected to that database. Not even from PgAdmin tool.
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    #now create a new database with utf8 encoding
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    #closing connection
    conn.close()
    #connecting to sparkify database
    conn = psycopg2.connect("host=localhost dbname=sparkifydb user=postgres password=************************")
    cur = conn.cursor()
    return cur, conn


# In[ ]:


def drop_tables(cur, conn):
    """
    here we will drop all the tables that are in drop table Query from sql_Queries.py to start from a fresh session.
    cur: cursor to the database
    conn: database connection reference
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


# In[ ]:


def create_tables(cur, conn):
    """
    Here we will create all the tables according to Queries in create_table_queries list from sql_queries.py
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


# In[ ]:


def main():
    """
    Main function for one final exection
    """
    cur,conn = create_database()
    
    drop_tables(cur, conn)
    print("Table dropped successfully!!")

    create_tables(cur, conn)
    print("Table created successfully!!")
    
    # finally closing connection
    conn.close()


if __name__ == "__main__":
    main()

