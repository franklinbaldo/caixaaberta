# reporter.py
import pandas as pd
import duckdb
import locale
import os # For joining path components

def format_currency(value):
    """Formats a float value as Brazilian Real currency string."""
    if pd.isna(value):
        return "N/A"
    try:
        # Set locale for Brazilian currency formatting if available
        # Trying common pt_BR locales
        for loc in ['pt_BR.UTF-8', 'pt_BR', 'Portuguese_Brazil.1252']:
            try:
                locale.setlocale(locale.LC_ALL, loc)
                # On some systems, locale.currency might not be enough,
                # and manual formatting is more reliable.
                # Using a simple f-string with comma for thousands and .2f for decimals.
                return f"R$ {value:,.2f}" 
            except locale.Error:
                continue
        # Fallback if pt_BR locale is not available
        return f"R$ {value:,.2f}"
    except Exception:
        return f"R$ {value:.2f}" # Basic fallback without thousands separator

DEFAULT_DB_PATH = os.path.join("dbt_real_estate", "real_estate_data.db")
DEFAULT_TABLE_NAME = "imoveis_geocoded" # Assumes this is the final, geocoded table

def generate_report(db_path: str = DEFAULT_DB_PATH, table_name: str = DEFAULT_TABLE_NAME):
    """
    Connects to the DuckDB database, queries the specified table (expected to be
    the geocoded properties data), calculates summary statistics, and prints them.

    Args:
        db_path (str): Path to the DuckDB database file.
        table_name (str): Name of the table to query within the database.
    """
    print(f"Generating report from DuckDB: {db_path}, Table: {table_name}")
    print("--------------------------------------------------")

    try:
        con = duckdb.connect(database=db_path, read_only=True)

        # Check if table exists
        table_check_query = f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}'"
        if not con.execute(table_check_query).fetchone():
            print(f"Error: Table '{table_name}' not found in the database '{db_path}'.")
            con.close()
            return

        # Fetch all data from the specified table
        df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
        con.close()

    except duckdb.Error as e:
        print(f"DuckDB error: {e}")
        return
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return

    if df.empty:
        print(f"Table '{table_name}' is empty or data could not be loaded. No statistics to generate.")
        return

    # Ensure 'price' (dbt model uses 'price') is numeric
    # The dbt model should already cast it, but good to be safe.
    if 'price' in df.columns:
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
    else:
        print(f"Warning: 'price' column (expected from dbt model 'imoveis_geocoded') is missing. Price statistics will be skipped.")
        df['price'] = pd.NA # Initialize to avoid errors

    # Ensure 'state' (dbt model uses 'state') column exists
    if 'state' not in df.columns:
        print(f"Error: 'state' column (expected from dbt model 'imoveis_geocoded') is missing. Report cannot be generated.")
        return

    print(f"Real Estate Data Report for dbt model '{table_name}':")
    print("--------------------------------------------------")

    total_properties = len(df)
    print(f"Total properties listed: {total_properties}")
    print("--------------------------------------------------")

    print("Properties per state:")
    properties_per_state = df.groupby('state').size()
    if properties_per_state.empty:
        print("  No data available for properties per state.")
    else:
        for state_code, count in properties_per_state.sort_index().items():
            print(f"  {state_code}: {count} properties")
    print("--------------------------------------------------")

    print("Average price per state:")
    if 'price' in df.columns and df['price'].notna().any():
        average_price_per_state = df.groupby('state')['price'].mean()
        if average_price_per_state.empty:
            print("  No price data available for calculating averages per state.")
        else:
            for state_code, avg_price in average_price_per_state.sort_index().items():
                print(f"  {state_code}: {format_currency(avg_price)}")
    else:
        print("  'price' column is missing or contains no valid data, cannot calculate average prices.")
    print("--------------------------------------------------")

    print("Geocoding Statistics:")
    print("--------------------------------------------------")
    if 'latitude' not in df.columns or 'longitude' not in df.columns:
        print("  Latitude/Longitude columns not found. Geocoding statistics cannot be generated.")
    else:
        total_geocoded = df['latitude'].notna().sum()
        percentage_geocoded_overall = (total_geocoded / total_properties) * 100 if total_properties > 0 else 0
        
        print(f"Overall geocoding success rate: {percentage_geocoded_overall:.1f}% ({total_geocoded} out of {total_properties} properties)")
        
        if total_properties > 0: # Only show per-state if there's data
            print("\nGeocoding success rate per state:")
            # df.groupby('state')['latitude'].count() counts non-NaN latitudes
            geocoded_per_state_counts = df.groupby('state')['latitude'].apply(lambda x: x.notna().sum())

            state_stats_df = pd.DataFrame({
                'total': properties_per_state, # Already calculated: df.groupby('state').size()
                'geocoded': geocoded_per_state_counts
            }).fillna(0) # Ensure states with no geocoded entries but present in total get 0

            # Ensure all states from properties_per_state are in geocoded_per_state_counts, even if with 0
            state_stats_df['geocoded'] = state_stats_df['geocoded'].astype(int)
            state_stats_df['total'] = state_stats_df['total'].astype(int)

            state_stats_df['percentage'] = (state_stats_df['geocoded'] / state_stats_df['total'] * 100).fillna(0)

            if state_stats_df.empty:
                print("  No per-state geocoding data available.")
            else:
                for state_code, row in state_stats_df.sort_index().iterrows():
                    print(f"  {state_code}: {row['percentage']:.1f}% ({row['geocoded']} out of {row['total']} properties)")
        else:
            print(" No properties to analyze for per-state geocoding success.")

    print("--------------------------------------------------")

def main():
    # The script now directly uses the DuckDB database and the specific table.
    # Command-line arguments for db_path or table_name could be added here with argparse if needed.
    generate_report()

if __name__ == "__main__":
    main()
