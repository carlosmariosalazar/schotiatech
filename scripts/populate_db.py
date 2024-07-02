import mysql.connector
from pathlib import Path
from dotenv import load_dotenv
from mysql.connector import Error
from utils import RegisterGenerator, read_json, get_env_variables


# Define paths
root_dir = Path()
config_path = root_dir / "config" / "generator.json"
schema_path = root_dir / "config" / "schema.json"

# Get environment variables
load_dotenv()
env_vars = get_env_variables()

# Read config
config = read_json(config_path)
n_samples = config["n_samples"]
batch_size = config["batch_size"]

# Insert data into database
def insert_data_in_db(n_samples: int, batch_size: int) -> None:

    data_generator = RegisterGenerator()
    
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

            for i,_ in enumerate(range (0,n_samples, batch_size)):
                data = []
                for _ in range(batch_size):
                    data.append(data_generator.get_sample())

                cursor.executemany("""INSERT INTO clients (period, status, city_client) 
                                   VALUES (%s, %s, %s)""", data)

                if (len(data) == cursor.rowcount):
                    connection.commit()
                    print(f"INFO : Batch {i}, {len(data)} rows inserted")
                else:
                    connection.rollback()
            
    except Error as ex:
        print(f"Error during connection {ex}")

    finally:
        if connection.is_connected():
            connection.close()

if __name__ == "__main__":
    insert_data_in_db(n_samples, batch_size)