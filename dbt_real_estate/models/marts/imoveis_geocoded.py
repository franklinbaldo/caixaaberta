import pandas as pd
import os
import sys

# This is a common way to handle imports from a parent directory in dbt Python models.
# It assumes 'src' is at the same level as 'dbt_real_estate'.
# Adjust if your project structure is different.
# sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'src'))
# Correcting the path to be relative to the dbt project root for dbt execution
# The dbt Python model will be executed from the root of the dbt project.
# So, if geocoding_utils.py is in `src` at the repo root, and dbt project is in `dbt_real_estate`,
# then the path needs to go up one level from dbt_real_estate to the repo root, then into `src`.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..')))


try:
    from src.geocoding_utils import get_coordinates_for_address
except ImportError as e:
    raise ImportError(f"Could not import get_coordinates_for_address from src.geocoding_utils. Error: {e}. Ensure 'src' directory is at the project root and geocoding_utils.py is accessible. Current sys.path: {sys.path}")


def model(dbt, session):
    dbt.config(materialized="table")

    # Get the upstream model (staging model) as a DataFrame
    stg_imoveis_df = dbt.ref("stg_imoveis").df()

    if stg_imoveis_df.empty:
        expected_cols = list(stg_imoveis_df.columns) + ['full_address', 'latitude', 'longitude']
        # Ensure the session.create_dataframe call matches the expected schema for an empty table
        # For DuckDB, providing an empty list of lists and column names is typical.
        # Or return an empty pandas DataFrame if that's what dbt expects for Python models.
        return pd.DataFrame(columns=expected_cols)


    geocoder_api_key = os.getenv("GEOCODER_KEY")
    if not geocoder_api_key:
        print("Warning: GEOCODER_KEY not set. Geocoding will be skipped; latitude and longitude will be null.")
        # Add full_address, latitude, and longitude columns with None if they don't exist
        # to maintain a consistent schema.
        if 'full_address' not in stg_imoveis_df.columns:
             stg_imoveis_df['full_address'] = None
        if 'latitude' not in stg_imoveis_df.columns:
            stg_imoveis_df['latitude'] = pd.NA
        if 'longitude' not in stg_imoveis_df.columns:
            stg_imoveis_df['longitude'] = pd.NA
        return stg_imoveis_df


    address_cols = ['address', 'neighborhood', 'city', 'state']
    for col in address_cols:
        if col not in stg_imoveis_df.columns:
            stg_imoveis_df[col] = "" # Create empty column if missing
        else:
            stg_imoveis_df[col] = stg_imoveis_df[col].fillna('').astype(str) # Fill NA and ensure string type

    stg_imoveis_df['full_address'] = stg_imoveis_df[address_cols].agg(
        lambda x: ', '.join(val.strip() for val in x if val.strip()), # Join non-empty, stripped strings
        axis=1
    )
    # Clean up excessive commas resulting from empty intermediate fields
    stg_imoveis_df['full_address'] = stg_imoveis_df['full_address'].str.replace(r'(,\s*)+', ', ', regex=True).str.strip(', ')


    coords_list = []
    # Limit geocoding for testing/CI if a flag is set, e.g., GEOCODING_LIMIT
    geocoding_limit = os.getenv("GEOCODING_LIMIT")
    limit = int(geocoding_limit) if geocoding_limit else None

    rows_to_geocode = stg_imoveis_df
    if limit is not None:
        print(f"Applying geocoding limit: processing only first {limit} rows.")
        rows_to_geocode = stg_imoveis_df.head(limit)
        # For rows beyond the limit, append (None, None)
        remaining_rows_count = len(stg_imoveis_df) - limit
        if remaining_rows_count < 0: remaining_rows_count = 0 # handle if limit > df length

    print(f"Attempting to geocode {len(rows_to_geocode)} addresses...")
    for i, address_str in enumerate(rows_to_geocode['full_address']):
        if pd.notna(address_str) and address_str.strip():
            print(f"Geocoding ({i+1}/{len(rows_to_geocode)}): {address_str[:100]}") # Log address being geocoded
            coords = get_coordinates_for_address(address_str, api_key=geocoder_api_key)
            coords_list.append(coords)
        else:
            coords_list.append((None, None))

    # If there was a limit, fill the rest with Nones
    if limit is not None and remaining_rows_count > 0:
        coords_list.extend([(None, None)] * remaining_rows_count)


    coords_df = pd.DataFrame(coords_list, index=stg_imoveis_df.index, columns=['latitude', 'longitude'])

    final_df = pd.concat([stg_imoveis_df, coords_df], axis=1)

    final_df['latitude'] = pd.to_numeric(final_df['latitude'], errors='coerce').astype('float64').where(pd.notnull(final_df['latitude']), None)
    final_df['longitude'] = pd.to_numeric(final_df['longitude'], errors='coerce').astype('float64').where(pd.notnull(final_df['longitude']), None)

    print(f"Geocoding complete. {final_df['latitude'].notna().sum()} coordinates found.")
    return final_df
