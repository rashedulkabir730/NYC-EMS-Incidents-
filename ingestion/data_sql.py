import duckdb
import logging
import uuid
import pandas as pd 


def load_data_duckdb(raw_df):

    run_id=uuid.uuid4()
    raw_df['run_id'] = run_id
    raw_df['ingestion_time'] = pd.Timestamp.now()
    raw_df['run_id'] = raw_df['run_id'].astype('string')

    con = duckdb.connect('data/raw.duckdb')
    con.execute("DROP TABLE IF EXISTS raw_api_data;")

    table_exists = con.execute("""
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'raw_api_data'
    """).fetchone()
    
    if table_exists is None:
        con.execute("""
            CREATE TABLE raw_api_data AS
            SELECT * FROM raw_df
            where 1=0
        """)
        con.append("raw_api_data",raw_df)

        logging.info(f"Created table with {len(raw_df)} records into DuckDB")
    else:
        con.append("raw_api_data",raw_df)

    
    con.close()