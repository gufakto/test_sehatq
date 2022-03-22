from sqlalchemy.engine import create_engine
import pandas as pd
from airflow.models import Variable

# ENV
DATABASE_SERVER = Variable.get("DATABASE_SERVER")
DATABASE_LOGIN = Variable.get("DATABASE_LOGIN")
DATABASE_PASSWORD = Variable.get("DATABASE_PASSWORD")
SC_DATABASE_NAME = Variable.get("SC_DATABASE_NAME")
DW_DATABASE_NAME = Variable.get("DW_DATABASE_NAME")


# Source Connection
sc_url_connection = "mysql+mysqlconnector://{}:{}@{}/{}".format(
                str(DATABASE_LOGIN), str(DATABASE_PASSWORD), str(
                    DATABASE_SERVER), str(SC_DATABASE_NAME)
)
sc_engine = create_engine(sc_url_connection)

# DW Connection
dw_url_connection = "mysql+mysqlconnector://{}:{}@{}/{}".format(
                str(DATABASE_LOGIN), str(DATABASE_PASSWORD), str(
                    DATABASE_SERVER), str(DW_DATABASE_NAME)
)
dw_engine = create_engine(dw_url_connection)

def table_to_csv(query: str, path: str):
    # Read data from sakila DB source 
    # set into DataFrame
    # save into temporary csv files
    df_ = pd.read_sql_query(query, sc_engine)
    df_.to_csv(path, index=False)

def csv_to_table(table, path: str):
    # read data from temporary csv files
    # save into sakila DW 
    df_ = pd.read_csv(path)
    df_.to_sql(con=dw_engine, name=table, if_exists='replace',chunksize=100)

def csv_to_df(path):
    return pd.read_csv(path, sep=",")    
