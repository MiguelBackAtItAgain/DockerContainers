import docker
from docker.errors import DockerException, NotFound

client = docker.from_env()

class mysql:
    def __init__(self, cn_name):
        self.cn_name = cn_name

    def create(self, db_password):
        print("Pulling MySQL image....")
        client.images.pull('mysql')
        print("Running MySQL container...")
        container = client.containers.run(
            'mysql',
            name = self.cn_name,
            environment={"MYSQL_ROOT_PASSWORD": db_password},
            ports={'3306/tcp': 3306},
            detach=True
        )
        print('MySQL container is running')
        
    def stop(self):
        try:
            container = client.containers.get(self.cn_name)
            container.stop()
        except NotFound:
            print("Container not found")
        except DockerException as e:
            print('An error occured when stopping the container')
    
    def delete(self):
        try:
            container = client.containers.get(self.cn_name)
            container.remove(force=True)
        except NotFound:
            print(f"Container not found.")
        except DockerException as e:
            print(f"An error occurred: {e}")

class mongo:
    def __init__(self, cn_name):
        self.cn_name = cn_name

    def create(self):
        print("Pulling MongoDB image...")
        client.images.pull("mongo")
        print("Creating MongoDB container...")
        container = client.containers.run(
            'mongo',
            name = self.cn_name,
            ports = {'27017/tcp': 27017},
            detach = True
        )
        print("MongoDB container is running")
    
    def stop(self):
        try:
            container = client.containers.get(self.cn_name)
            container.stop()
        except NotFound:
            print("Container not found")
        except DockerException as e:
            print("An error occured when stopping the container. ")
        
    def delete(self):
        try:
            container = client.containers.get(self.cn_name)
            container.remove(force=True)
        except NotFound:
            print(f"Container not found.")
        except DockerException as e:
            print(f"An error occurred: {e}")

class redis:
    def __init__(self, cn_name):
        self.cn_name = cn_name
    
    def create(self):
        print("Pulling Redis image...")
        client.images.pull("redis")
        print("Running Redis container...")
        container = client.containers.run(
            'redis',
            name = self.cn_name,
            ports = {'6379/tcp': 6379},
            detach = True
        )
        print("Cassandra container is running")

    def stop(self):
        try:
            container = client.containers.get(self.cn_name)
            container.stop()
        except NotFound:
            print("Container not found")
        except DockerException as e:
            print("An error occured when stopping the container.")

    def delete(self):
        try:
            container = client.containers.get(self.cn_name)
            container.remove(force=True)
        except NotFound:
            print(f"Container not found.")
        except DockerException as e:
            print(f"An error occurred: {e}")

class cassandra:
    def __init__(self, cn_name):
        self.cn_name = cn_name
    
    def create(self):
        print("Pulling Cassandra image...")
        client.images.pull("cassandra")
        print("Running Cassandra container...")
        container = client.containers.run(
            'cassandra',
            name = self.cn_name,
            ports = {'9042/tcp': 9042},
            detach = True
        )
        print("Cassandra container is running")

    def stop(self):
        try:
            container = client.containers.get(self.cn_name)
            container.stop()
        except NotFound:
            print("Container not found")
        except DockerException as e:
            print("An error occured when stopping the container. ")

    def delete(self):
        try:
            container = client.containers.get(self.cn_name)
            container.remove(force=True)
        except NotFound:
            print(f"Container not found.")
        except DockerException as e:
            print(f"An error occurred: {e}")
