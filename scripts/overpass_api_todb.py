# %%
import requests
import duckdb
import pandas as pd
import time

# this is function part
def fetch_overpass_data(url, payload):
    rows = []
    try:
        response = requests.post(url, data=payload)
        data = response.json()
        elements = data.get('elements', [])
        if elements:
            named_elements = [e for e in elements if e.get('tags', {}).get('name')] 
            for element in named_elements:
                element_id = element.get('id')
                element_amenity = element.get('tags', {}).get('amenity','N/A')
                element_name = element.get('tags', {}).get('name', 'N/A')
                latitude = element.get('lat')
                longitude = element.get('lon')
                rows.append({
                    'id': element_id,
                    'amenity': element_amenity,
                    'name': element_name,
                    'latitude': latitude,
                    'longitude': longitude
                })
            return pd.DataFrame(rows)
        else:
            print("No elements found matching the query criteria.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error in requests : {e}")
        return None

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
COUNTRY_ISO_CODE = "MM" # Use the ISO code for country
db_file = 'risk_analytics_mm.duckdb'
table_name = 'mm_infrastructure'

# %%
with duckdb.connect(database=db_file) as conn:
    conn.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id BIGINT PRIMARY KEY,
            amenity VARCHAR,
            name VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE
        )
    ''')
# %% 
query_hospital = f"""
// default timeout is 180 seconds
[date:"2025-03-01T00:00:00Z"][out:json][timeout:180];
area["boundary"="administrative"]["admin_level"="2"]["ISO3166-1"="{COUNTRY_ISO_CODE}"]->.ctry;
(
  // Find nodes (points), ways (lines/polygons), and relations (complex areas) 
  node["amenity"="hospital"](area.ctry);
  node["healthcare"](area.ctry);
  //  way["amenity"="hospital"](area.ctry);
  //  relation["amenity"="hospital"](area.ctry);
);
// Output the results with full geometry
out body geom;
>;
out skel qt;
"""
query_school = f"""
// default timeout is 180 seconds
[date:"2025-03-01T00:00:00Z"][out:json][timeout:180];
area["boundary"="administrative"]["admin_level"="2"]["ISO3166-1"="{COUNTRY_ISO_CODE}"]->.ctry;
(
  // Find nodes (points), ways (lines/polygons), and relations (complex areas) 
  node["amenity"="school"](area.ctry);
  node["amenity"="university"](area.ctry);
);
// Output the results with full geometry
out body geom;
>;
out skel qt;
"""
payload1 = {'data': query_hospital}
payload2 = {'data': query_school}

# %%
hospital_df = fetch_overpass_data(OVERPASS_URL, payload1)
time.sleep(10) 
school_df = fetch_overpass_data(OVERPASS_URL, payload2)
# %%
with duckdb.connect(db_file) as conn:
    conn.register('df1_view', hospital_df)
    conn.execute(f"insert into {table_name} select * from df1_view ON CONFLICT (id) DO NOTHING")
    conn.register('df2_view', school_df)                
    conn.execute(f"insert into {table_name} select * from df2_view ON CONFLICT (id) DO NOTHING")

# %% --- IGNORE ---
