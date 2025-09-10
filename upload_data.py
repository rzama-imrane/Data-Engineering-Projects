
import argparse
import os 
import pandas as pd
from sqlalchemy import create_engine
from time import time

def main(parameters):

    user = parameters.user
    password = parameters.password
    db = parameters.db
    host = parameters.host
    port = parameters.port
    table_name = parameters.table_name
    url = parameters.url
    
    parquet_name = "output.parquet"

    os.system(f"curl -o {parquet_name} {url} ")
    
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()


    df = pd.read_parquet(parquet_name)


    df.tpep_pickup_datetime = pd.to_datetime( df.tpep_pickup_datetime, infer_datetime_format = True )
    df.tpep_dropoff_datetime = pd.to_datetime( df.tpep_dropoff_datetime , infer_datetime_format = True)
    
    df.head(0).to_sql(table_name, if_exists="replace", con= engine)
    
    
    t_start = time()
    
    df.to_sql(table_name, if_exists="append", con= engine)

    t_end = time()

    print(" inserted another chunk time spent : %.3f second" % (t_end -t_start))
    
if __name__ == "__main__" :
    
    parser = argparse.ArgumentParser()

    parser.add_argument("--user", help= "user name for postgres")
    parser.add_argument("--password", help= "password for postgres")
    parser.add_argument("--db", help= "database name for postgres")
    parser.add_argument("--host", help= "host for postgres")
    parser.add_argument("--port", help= "port for postgres")
    parser.add_argument("--url", help= "url for the csv")
    parser.add_argument("--table_name", help = "name of the table we are ingesting data to")

    args = parser.parse_args()

    main(args)



