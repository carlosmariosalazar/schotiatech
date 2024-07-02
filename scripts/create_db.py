import mysql.connector
from pathlib import Path
from dotenv import load_dotenv
from mysql.connector import Error
from utils import read_json, get_env_variables

# Acces to environment variables
load_dotenv()
env_vars = get_env_variables()

# Define directories and paths
root_dir = Path()
schema_path = root_dir / "config" / "schema.json"

# Create table sentence
def create_table_sentence(schema: dict) -> str:
    for table,value in schema.items():
        definitions = []
        for entity, definition in value.items():
            definitions.append(entity + " " + definition)

    sentence = f"CREATE TABLE IF NOT EXISTS {table} ({','.join(definitions)})"
    return sentence

# Create database and table
def create_db(schema_path: Path) -> None:
    
    schema = read_json(schema_path)
    sentence_table = create_table_sentence(schema)

    try: 
        connection = mysql.connector.connect(
            host= env_vars["DB_HOST"],
            user= env_vars["DB_USER"],
            database= env_vars["DB_NAME"],
            password= env_vars["DB_PASSWORD"],
            port= env_vars["DB_PORT"]
            )
        
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(f"""CREATE DATABASE IF NOT EXISTS {env_vars['DB_NAME']}""")
            cursor.execute(sentence_table)      
            print("Ok!")

    except Error as ex:
        print(f"Error during connection {ex}")

    finally:
        if connection.is_connected():
            connection.close()


if __name__ == "__main__":
    create_db(schema_path)
