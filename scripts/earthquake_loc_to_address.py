# %%
import requests
import json
import time
import duckdb
import pandas as pd

db_file = 'risk_analytics_mm.duckdb'
input_table = 'usgs_earthquakes'
output_table = 'earthquake_township'

nominatim_base_url = "https://nominatim.openstreetmap.org/reverse"
output_format = 'json'
zoom = 12           # township level
address_details = 1
delay_seconds = 1.5 

# %%
def get_reverse_geocode(lat, lon):
    headers = {
     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en" 
    }
    params = {
        'lat': lat,
        'lon': lon,
        'zoom': zoom,
        'addressdetails': address_details,
        'format': output_format
    }

    try:
        response = requests.get(
            nominatim_base_url, 
            params=params,
            headers=headers,  
            timeout=15
        )

        response.raise_for_status()

        # Nominatim policy: 1 request per second 
        time.sleep(delay_seconds)

        return response.json()

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error at ({lat}, {lon}): {e}")
    except requests.exceptions.RequestException as e:
        print(f"Network Error at ({lat}, {lon}): {e}")
    except json.JSONDecodeError:
        print(f"JSON decode error at ({lat}, {lon})")
    return None

# %%
with duckdb.connect(database=db_file) as conn:
    conn.execute(f'''
                 CREATE TABLE IF NOT EXISTS {output_table} (
                 earthquake_id VARCHAR PRIMARY KEY, 
                 latitude DOUBLE, 
                 longitude DOUBLE,
                 township VARCHAR)
                 ''')
    
    query = f'''
            select id, latitude,longitude 
            from {input_table}
            '''
    # for connection tiemeout issues, retry only for those not in output table
    retry_query = f'''
    select id, latitude, longitude
    from {input_table} t1
    where not exists (select 1 from 
    earthquake_township t2
    where t2.earthquake_id = t1.id) 
    '''

    # df_coords = conn.execute(query).fetchdf()
    df_coords = conn.execute(retry_query).fetchdf()
    rows = []
    for i, row in df_coords.iterrows():
        earthquake_id = row['id']
        lat = row['latitude']
        lon = row['longitude']

        print(f"[{i+1}/{len(df_coords)}] Geocoding ({lat:.6f}, {lon:.6f})")
        # lat, lon = 16.8868893, 96.1035971
        data = get_reverse_geocode(lat, lon)
        if data:
            address = data.get("address", {})
            if address.get('municipality') is not None:
                township = str.lower(address['municipality'])
            elif address.get('suburb') is not None:
                    township = str.lower(address['suburb'])
            else:
                township = "not_found"
            rows.append({
                'earthquake_id': earthquake_id,
                'latitude': lat,
                'longitude': lon,
                'township': township
            })
    result_df = pd.DataFrame(rows)
    conn.register('result_view', result_df)  
    conn.execute(f'''
        INSERT INTO {output_table}
        SELECT * FROM result_view
        ON CONFLICT (earthquake_id) DO NOTHING
    ''')   
# %%
