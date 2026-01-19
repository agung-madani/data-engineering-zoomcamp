import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
FILE_NAME = "yellow_tripdata_2021-01.csv.gz"
CSV_URL = PREFIX + FILE_NAME

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table', default='yellow_taxi_data', help='Target table name')
def ingest_data(user, password, host, port, db, table):
    """
    Ingest NYC Taxi data into PostgreSQL
    """

    # -------------------------------------
    # Create database engine
    # -------------------------------------
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{db}?sslmode=disable"
    )

    # -------------------------------------
    # Read first chunk to create schema
    # -------------------------------------
    df_iter = pd.read_csv(
        CSV_URL,
        iterator=True,
        chunksize=100000,
        dtype=dtype,
        parse_dates=parse_dates
    )

    df = next(df_iter)

    print("Creating table schema...")

    df.head(0).to_sql(
        name=table,
        con=engine,
        if_exists="replace",
        index=False
    )

    print("Table created")

    # -------------------------------------
    # Insert first chunk
    # -------------------------------------
    df.to_sql(
        name=table,
        con=engine,
        if_exists="append",
        index=False
    )

    print(f"Inserted first chunk: {len(df)} rows")

    # -------------------------------------
    # Insert remaining chunks
    # -------------------------------------
    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(
            name=table,
            con=engine,
            if_exists="append",
            index=False
        )

    print("Data ingestion completed")

if __name__ == "__main__":
    ingest_data()
