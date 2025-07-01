import pandas as pd
import os
import sys

# This is a common way to handle imports of modules in sibling directories (e.g., src)
# when running dbt. dbt typically runs from the project root.
# Ensure 'src' is in the Python path.
# For dbt Cloud or dbt CLI execution, PYTHONPATH might need to be set,
# or the package structure adjusted (e.g. making 'src' a proper package installable by pip).
# For this project structure, adding to sys.path should work for local `dbt run`.

# Get the absolute path to the project root (assuming dbt_project.yml is in the root)
# The dbt execution directory is usually the dbt project root.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
src_path = os.path.join(project_root, 'src')

if src_path not in sys.path:
    sys.path.insert(0, src_path)

try:
    from geocoding_utils import get_coordinates_for_address
except ImportError:
    # This fallback is for when dbt is invoked in a way that src_path isn't correctly added.
    # It's a common issue with dbt Python models and local module imports.
    # A more robust solution is to package `src` or adjust PYTHONPATH environment variable.
    print("Attempting fallback import for geocoding_utils due to potential sys.path issue...")
    # This assumes 'src' is a sibling of 'dbt_real_estate' and current dir is 'dbt_real_estate/models/marts'
    # More robust: use environment variables or dbt project configurations if available.
    try:
        # This path assumes dbt is run from the root of the repository
        sys.path.append(os.path.join(os.getcwd(), 'src'))
        from geocoding_utils import get_coordinates_for_address
    except ImportError as e:
        raise ImportError(
            "Could not import get_coordinates_for_address from src.geocoding_utils. "
            "Ensure 'src' directory is in PYTHONPATH or accessible. "
            f"Current sys.path: {sys.path}. Error: {e}"
        )

def model(dbt, session):
    # dbt configuration
    dbt.config(
        materialized="table",
        packages=["pandas"] # Inform dbt this model uses pandas
    )

    # Get upstream data (from stg_imoveis)
    stg_imoveis_df = dbt.ref("stg_imoveis").df()

    if stg_imoveis_df.empty:
        # If there's no data, return an empty DataFrame with expected columns
        # This prevents errors in downstream processes if the source is unexpectedly empty.
        # Define columns based on stg_imoveis_df schema plus new geocoding columns
        # For simplicity, if stg_imoveis_df is empty, we'll just return it,
        # as it will be an empty df. The geocoding logic below handles empty df fine.
        # However, explicitly defining schema for empty return is more robust.
        print("Upstream model stg_imoveis is empty. Skipping geocoding.")
        # Add latitude and longitude columns if they don't exist, even for an empty DataFrame
        if 'latitude' not in stg_imoveis_df.columns:
            stg_imoveis_df['latitude'] = pd.NA
        if 'longitude' not in stg_imoveis_df.columns:
            stg_imoveis_df['longitude'] = pd.NA
        return stg_imoveis_df


    # Ensure latitude and longitude columns exist, initialize with pd.NA (Pandas native missing type)
    if 'latitude' not in stg_imoveis_df.columns:
        stg_imoveis_df['latitude'] = pd.NA
    if 'longitude' not in stg_imoveis_df.columns:
        stg_imoveis_df['longitude'] = pd.NA

    # Convert existing lat/lon to numeric, coercing errors to NA (handles empty strings or bad data)
    stg_imoveis_df['latitude'] = pd.to_numeric(stg_imoveis_df['latitude'], errors='coerce')
    stg_imoveis_df['longitude'] = pd.to_numeric(stg_imoveis_df['longitude'], errors='coerce')


    # Construct full address for geocoding
    # Only geocode rows where latitude is null
    rows_to_geocode = stg_imoveis_df['latitude'].isnull()

    if not rows_to_geocode.any():
        print("No rows require geocoding.")
        return stg_imoveis_df # Return the original DataFrame if no geocoding is needed

    # Create the 'full_address' string for rows that need geocoding
    # Ensure all address components are strings and handle potential NaN values by filling with empty string
    address_cols = ['endereco', 'bairro', 'cidade', 'estado']
    stg_imoveis_df['full_address'] = stg_imoveis_df[rows_to_geocode][address_cols].fillna('').astype(str).agg(', '.join, axis=1)

    # Retrieve GEOCODER_KEY from environment variable
    # dbt run will pass environment variables from where it's executed.
    # For GitHub Actions, ensure this secret is set as an env var for the dbt step.
    api_key = os.getenv("GEOCODER_KEY")
    if not api_key:
        print("Warning: GEOCODER_KEY environment variable not set. Geocoding may fail or be rate-limited.")

    # Apply geocoding
    # We only apply to the subset of rows that need geocoding for efficiency
    print(f"Starting geocoding for {rows_to_geocode.sum()} rows...")

    geocoded_coords = stg_imoveis_df.loc[rows_to_geocode, 'full_address'].apply(
        lambda addr: get_coordinates_for_address(addr, api_key=api_key) if pd.notna(addr) and addr.strip() else (None, None)
    )

    # Unpack the (latitude, longitude) tuples into two new columns and assign back
    # Only assign to the rows that were geocoded
    if not geocoded_coords.empty:
        coords_df = pd.DataFrame(geocoded_coords.tolist(), index=stg_imoveis_df[rows_to_geocode].index, columns=['latitude_new', 'longitude_new'])
        stg_imoveis_df.loc[rows_to_geocode, 'latitude'] = coords_df['latitude_new']
        stg_imoveis_df.loc[rows_to_geocode, 'longitude'] = coords_df['longitude_new']

    print("Geocoding complete.")

    # Drop the temporary 'full_address' column
    final_df = stg_imoveis_df.drop(columns=['full_address'], errors='ignore')

    return final_df

# Note on dbt Python model requirements:
# - The function must be named `model`.
# - It must take `dbt` and `session` as arguments.
# - It must return a Pandas DataFrame (or other supported type like PySpark DataFrame).
# - `dbt.ref("model_name")` is used to get a DataFrame from an upstream dbt model.
# - `dbt.source("source_name", "table_name")` for dbt sources.
# - `dbt.config(...)` to set model configurations.

# To run this model:
# 1. Ensure `src.geocoding_utils` is importable (PYTHONPATH or project structure).
# 2. Set `GEOCODER_KEY` environment variable if your geocoding service needs it.
# 3. Run `dbt run --models imoveis_geocoded` (or `dbt build`).
#
# Potential improvements:
# - Batch geocoding requests if the API supports it, for greater efficiency.
# - More sophisticated error handling and retry logic for geocoding API calls.
# - Caching results of geocoding (though geocoding_utils.py already does this).
# - Configurability of address columns.
