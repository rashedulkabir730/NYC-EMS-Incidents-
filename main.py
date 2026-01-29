from pull_data import pull_data
from data_sql import load_data_duckdb
import logging 

def main():
    raw_df = pull_data()
    logging.info(f'Final : {len(raw_df)}')
    load_data_duckdb(raw_df=raw_df)


if __name__ == "__main__":
    main()





