#%%
import requests
import duckdb
import pandas as pd

#%%
# In duckdb, default schema is 'main'
db_file = 'risk_analytics_mm.duckdb'
table_name = 'usgs_earthquakes'
    
#%%
usgs_api_url = 'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime=NOW-7DAYS&minmagnitude=2.5&minlatitude=9.78&maxlatitude=28.62&minlongitude=92.08&maxlongitude=101.28'

try:
    response = requests.get(usgs_api_url, timeout=10)
    geojson = response.json()
except requests.exceptions.RequestException as e:
    print(f"Error in requests : {e}")
# %%
rows = []
for feature in geojson['features']:
    properties = feature['properties']
    geometry = feature['geometry']
    coordinates = geometry['coordinates']
    rows.append({
        'id': feature['id'],
        'title' : properties['title'],
        'time': properties['time'], # unix timestamps in milliseconds
        'latitude': coordinates[1],
        'longitude': coordinates[0],
        'depth_km': coordinates[2],
        'magnitude': properties['mag'],
        'place': properties['place'],
        'detail': properties['detail']
    })

df = pd.DataFrame(rows)

# %%
with duckdb.connect(database=db_file) as conn:
    conn.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id VARCHAR PRIMARY KEY,
            title VARCHAR,
            time BIGINT,
            latitude DOUBLE,
            longitude DOUBLE,
            depth_km DOUBLE,
            magnitude DOUBLE,
            place VARCHAR,
            detail VARCHAR
        )
    ''')
    conn.register('df_view', df)
    conn.execute(f"insert into {table_name} select * from df_view ON CONFLICT (id) DO NOTHING")

