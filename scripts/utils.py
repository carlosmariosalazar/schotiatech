import os
import json
from faker import Faker
from typing import Tuple
from pathlib import Path
from datetime import datetime
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

# Set paths
root_dir = Path()
config_path = root_dir / "config" / "generator.json"

# Read JSON file
def read_json(path: Path) -> dict:
    with open(path, encoding= "UTF-8") as f:
        config = json.load(f)
    return config

# Get environment variables
def get_env_variables() -> dict:
    env_vars = {"DB_HOST": os.environ["DB_HOST"],
                "DB_PORT": os.environ["DB_PORT"],
                "DB_USER": os.environ["DB_USER"],
                "DB_NAME": os.environ["DB_NAME"],
                "DB_PASSWORD": os.environ["DB_PASSWORD"]}
    return env_vars

# Data generator
class RegisterGenerator():
    def __init__(self):
        # Settings
        Faker.seed(450)
        self.config = read_json(config_path)
        self.fake = Faker(self.config["country"])

        self.start_date = datetime.strptime( self.config["datetime"]["start_date"],"%Y-%M-%d")
        self.end_date = datetime.strptime( self.config["datetime"]["end_date"],"%Y-%M-%d")

        self.statuses = self.config["status"]
    
    def get_sample(self) -> Tuple[datetime, str, str]:
        # Generate random samples
        period = self.fake.date_time_between(self.start_date, self.end_date) 
        status = self.fake.random_element(self.statuses)
        city_client = self.fake.address()
        city_client = "".join(city_client.split("\n")[-1:])

        return period, status, city_client
    

# Drop table from database
def drop_table() -> None:

    load_dotenv()
    env_vars = get_env_variables()

    try:
        connection = mysql.connector.connect(
            host= env_vars["DB_HOST"],
            port= env_vars["DB_PORT"],
            user= env_vars["DB_USER"],
            password= env_vars["DB_PASSWORD"],
            database= env_vars["DB_NAME"]
            )
        
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("""DROP TABLE IF EXISTS clients""")
            connection.commit()
            print(f"INFO : TABLE HAS BEEN DROPPED")

    except Error as ex:
        print(f"Error during connection {ex}")

    finally:
        if connection.is_connected():
            connection.close()

if __name__ == "__main__":
    drop_table()
    #generator = RegisterGenerator()
    #print(generator.get_sample())
    