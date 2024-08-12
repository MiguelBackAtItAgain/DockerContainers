from container import mongo, mysql, redis, cassandra
from db_ops import mongo_db, mysql_db, redis_db, cassandra_db
from threading import Timer
from cassandra.cluster import NoHostAvailable
import time
import sys


def clearout():
    print("Stopping MySQL container...")
    mysql_container = mysql('some-mysql')
    mysql_container.stop()
    print("Deleting MySQL container...")
    mysql_container.delete()
    print("MySQL container deleted successfully!")
    print("Stopping Mongo container...")
    mongo_container = mongo('some-mongo')
    mongo_container.stop()
    print("Deleting Mongo container...")
    mongo_container.delete()
    print("Mongo container deleted successfully!")
    redis_container = redis('some-redis')
    print('Stopping Redis container...')
    redis_container.stop()
    print('Deleting Redis container...')
    redis_container.delete()
    print("Stopping Cassandra container...")
    css_container = cassandra('some-cassandra')
    css_container.stop()
    print("Deleting Cassandra container...")
    css_container.delete()
    print('All containers have been deleted, ready to start.')
    

argument = len(sys.argv)
if (argument > 1):
    argument = sys.argv[1]

if(argument == '-clear'):
    clearout()
    sys.exit()

def mysql_startup():
    print("Creating MySQL container...")
    mysql_container = mysql('some-mysql')
    mysql_container.create('MySecretPassword')
    print("Creating 'pluto' database...")
    time.sleep(20)
    sql = mysql_db('MySecretPassword')
    sql.create()
    print("All done!")

def mongo_startup():
    print("Creating MongoDB container ...")
    mongo_container = mongo('some-mongo')
    mongo_container.create()
    print("Creating client...")
    mng = mongo_db()
    print("All done!")

def redis_startup():
    print("Creating Redis container...")
    redis_container = redis('some-redis')
    redis_container.create()
    print("Creating client...")
    rds = redis_db()
    print("All done!")

def cassandra_startup(retries=10, delay=20):
    print("Creating Cassandra container...")
    css_container = cassandra('some-cassandra')
    css_container.create()

    for attempt in range(retries):
        try:
            print(f'Attempting to create client... (Attempt {attempt + 1} of {retries})')
            css = cassandra_db()
            css.create()
            print('Cassandra setup completed successfully.')
            break  
        except NoHostAvailable:
            print(f'Connection attempt {attempt + 1} failed. Retrying in {delay} seconds...')
            time.sleep(delay)
    else:
        print('Failed to connect to Cassandra after multiple attempts.')

def mysql_write():
    db = mysql_db('MySecretPassword')
    db.write()

def mongodb_write():
    db = mongo_db()
    db.write()

def redis_write():
    rds = redis_db()
    rds.write()

def cassandra_write():
    css = cassandra_db()
    css.write()

def verify():
    db = mysql_db('MySecretPassword')
    m_db = mongo_db()
    r_db = redis_db()
    css_db = cassandra_db()
    print("--------------------------------------------")
    print("Last 5 elements added to MySQL 'pluto' database:")
    db.read()
    print("--------------------------------------------")
    print("Last 5 elements added to Mongo 'pluto' collection:")
    m_db.read()
    print("--------------------------------------------")
    print("Last 5 elements added to Redis database:")
    r_db.read()
    print("--------------------------------------------")
    print("Last 5 elements added to Cassandra database:")
    css_db.read()    

def timeloop(): 
    while True:
        print(f'----------- LOOP: ' + time.ctime() + ' ---------------')
        mysql_write()
        mongodb_write()
        redis_write()
        cassandra_write()
        verify()
        print("Reseting database connections...")
        time.sleep(20)

mysql_startup()
mongo_startup()
redis_startup()
cassandra_startup()
timeloop()