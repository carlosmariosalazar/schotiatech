import mysql.connector
from pathlib import Path
from mysql.connector import Error
import numpy as np
from dotenv import load_dotenv
import pandas as pd
from utils import get_env_variables, read_json

# Get environment variables
load_dotenv()
env_vars = get_env_variables()

# Define paths
root_dir = Path()
data_path = root_dir / "data" / "data.csv"
reg_ts_path = root_dir / "data" / "reg_ts.csv"
acr_ts_path = root_dir / "data" / "acr_ts.csv"
departments_config_path = root_dir / "config" /"department_capitals.json"
departments_code_path = root_dir / "config" / "department_code.json"


# Get data from database
def get_raw_data() -> pd.DataFrame:

    query = "SELECT * FROM clients"

    # Database connection, query execution and fetch results
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
            cursor.execute(query)
            result = cursor.fetchall()
            print("INFO: Query executed")

    except Error as ex:
        print(f"Error during connection {ex}")

    finally:
        if connection.is_connected():
            connection.close()

    # Create DataFrame from result
    df = pd.DataFrame(data= result, columns= ["id","period","status","city_client"])
    df = df.set_index("id")
    return df

def process_data(df: pd.DataFrame, output_path: Path|None = None, save: bool = False) -> pd.DataFrame:

    data = df.copy()
    departments_dict = read_json(departments_config_path)
    department_code = read_json(departments_code_path)
    mapping_dict = {"active": 1,"inactive": 0} 
    
    data[["city_client", "department_client"]] = data["city_client"].str.split(", ", expand= True)
    data.loc[data["department_client"] == "D.C.", "department_client"] = "Cundinamarca"

    data["status_str"] = data["status"].copy()
    data["status"] = data["status"].map(mapping_dict)
    data["department_client"] = data["department_client"].fillna(data["city_client"].map(departments_dict))
    data["department_code"] = data["department_client"].map(department_code)
    data["country_client"] = "Colombia"

    if save:
        data.to_csv(output_path)
    return data

# Get client registration count time series
def get_registration_ts(df: pd.DataFrame, output_path: Path|None = None, save: bool= False) -> None:
    data = df.copy()
    tmp = data.groupby(pd.Grouper(key= "period", freq= "D")).size()
    tmp = pd.DataFrame(data= tmp, columns= ["count"])
    
    if save:
        tmp.to_csv(output_path)
        print("INFO: Registration Time Series saved!")

# Get mean active client rate time series
def get_acr_ts(df: pd.DataFrame, output_path: Path|None = None, save: bool= False) -> None:
    data = df.copy()
    tmp = data.groupby(pd.Grouper(key= "period", freq= "D"))["status"].mean().to_frame()
    tmp = tmp.rename(columns= {"status": "acr"})
    tmp = tmp.fillna(0)

    
    if save:
        tmp.to_csv(output_path)
        print("INFO: ACR Time Series saved!")

if __name__ == "__main__":

    # Get raw dataframe from query
    df_raw = get_raw_data()

    # Process the data and save file
    df = process_data(df= df_raw, output_path= data_path, save= True)

    # Get registration count time series and save file
    get_registration_ts(df= df, output_path= reg_ts_path, save= True)

    # Get Active client Rate time series and save file
    get_acr_ts(df= df, output_path= acr_ts_path, save= True)

