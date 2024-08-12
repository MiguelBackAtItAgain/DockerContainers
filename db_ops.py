import mysql.connector
import pymongo
from pymongo import MongoClient
from cassandra.cluster import Cluster
import redis
from datetime import datetime
import uuid

class mysql_db:
    def __init__(self, db_password):
        self.db_password = db_password
        self.connection = mysql.connector.connect(  
                                                    user='root',
                                                    password=db_password,
                                                    host='127.0.0.1',
                                                    port='3306',
                                                    database="",
                                                    auth_plugin='mysql_native_password'
                                                )
        self.cursor = self.connection.cursor()

    def create(self):
        query = ("DROP DATABASE IF EXISTS `pluto`;")
        self.cursor.execute(query)

        query = ("CREATE DATABASE IF NOT EXISTS pluto")
        self.cursor.execute(query)

        query = ("USE pluto")
        self.cursor.execute(query)

        query = ('''
        CREATE TABLE posts(
            id VARCHAR(36),
            stamp VARCHAR(20)
        )
        ''')
        self.cursor.execute(query)

        self.connection.commit()
        self.cursor.close()
        
    def write(self):
        query = ("USE pluto")
        self.cursor.execute(query)
        id = str(uuid.uuid4())
        time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        query = (f'INSERT INTO `posts` VALUES("{id}","{time}")')
        self.cursor.execute(query)
        self.connection.commit()
        self.cursor.close()
        print(f"Data added to the MySQL database: [{id}], [{time}]")

    def read(self):
        query = ("USE pluto")
        self.cursor.execute(query)
        query = ("SELECT * FROM posts ORDER BY stamp DESC LIMIT 5;")
        self.cursor.execute(query)
        stamps = []
        for row in self.cursor.fetchall():
            print(row)

    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("Database connection terminated")

class mongo_db:
    def __init__(self):
        self.client = MongoClient("mongodb://localhost:27017/dockerdemo")
        self.db = self.client.pluto

    def write(self):
        time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        item = {
            'time': time
        }
        self.db.posts.update_one(item, {'$set': item}, upsert=True)
        print(f"Data added to the Mongo collection: [{id}], [{time}]")
    
    def read(self):
        items = []
        for post in self.db.posts.find().sort('id', pymongo.DESCENDING).limit(5):
            print(post)
    
    def delete(self):
        self.db.posts.delete_many({})

class cassandra_db:
    def __init__(self):
        self.cluster = Cluster(['localhost'],port=9042)
        self.session = self.cluster.connect()

    def create(self):
        self.session.execute("""CREATE KEYSPACE IF NOT EXISTS pluto WITH REPLICATION = {
                              'class': 'SimpleStrategy', 'replication_factor': 1 }""")
        self.session.set_keyspace('pluto')

        self.session.execute("""
            CREATE TABLE IF NOT EXISTS post (
                id text PRIMARY KEY,
                stamp text
            );
        """
        )
        self.session.shutdown()
        self.cluster.shutdown()

    def write(self):
        self.session.execute('USE pluto')
        id = str(uuid.uuid4())
        time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.session.execute("""INSERT INTO post (id, stamp) VALUES (%s, %s)""", (id, time))
        print(f"Data added to the Cassandra database: [{id}], [{time}]")
        self.session.shutdown()
        self.cluster.shutdown()

    
    def read(self):
        self.session.execute('USE pluto')
        rows = self.session.execute('SELECT * FROM post')
        for row in rows:
            print(row)
        self.session.shutdown()
        self.cluster.shutdown()
    
    def delete(self):
        query = f"DELETE FROM pluto"
        self.session.execute(query)
        self.session.shutdown()
        self.cluster.shutdown()

class redis_db:
    def __init__(self):
        self.r = redis.Redis(host='localhost', port=6379, db=0)
    
    def write(self):
        id = str(uuid.uuid4())
        time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        values = {
            "id": id,
            "time": time
        }
        self.r.mset(values)
        print(f"Data added to the Redis database: [{id}], [{time}]")
    
    def read(self):
        cursor = '0'
        while cursor != 0:
            cursor, keys = self.r.scan(cursor=cursor, count=10)
            for key in keys:
                value = self.r.get(key)
                print(f"{key.decode('utf-8')}: {value.decode('utf-8')}")
    
    def delete(self):
        keys = self.r.keys('*')
        if keys:
            self.r.delete(*keys)
            print("All keys deleted.")
        else:
            print("No keys found to delete.")




