import duckdb


con = duckdb.connect("data/raw.duckdb")
#con.execute("DROP TABLE IF EXISTS raw_api_data")
#print(con.execute("PRAGMA table_info('raw_api_data')").fetchall())

print(con.execute("SHOW TABLES").fetchall())
