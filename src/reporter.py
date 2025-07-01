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

    # Ensure 'preco' is numeric, handling potential non-numeric data if schema wasn't strictly enforced
    if 'preco' in df.columns:
        df['preco'] = pd.to_numeric(df['preco'], errors='coerce')
    else:
        print(f"Warning: 'preco' column is missing from table '{table_name}'. Price statistics will be skipped.")
        # Initialize preco with NAs if missing, so downstream code doesn't break, but stats will be NA/0
        df['preco'] = pd.NA

    if 'estado' not in df.columns:
        print(f"Error: 'estado' column is missing from table '{table_name}'. Report cannot be generated.")
        return

    print(f"Real Estate Data Report for dbt model '{table_name}':")
    print("--------------------------------------------------")

    # Total number of properties
    total_properties = len(df)
    print(f"Total properties listed: {total_properties}")
    print("--------------------------------------------------")

    # Number of properties per state
    print("Properties per state:")
    properties_per_state = df.groupby('estado').size()
    if properties_per_state.empty:
        print("  No data available for properties per state.")
    else:
        for state, count in properties_per_state.items():
            print(f"  {state}: {count} properties")
    print("--------------------------------------------------")

    # Average price of properties per state
    print("Average price per state:")
    # Ensure 'preco' column exists before attempting to calculate mean
    if 'preco' in df.columns:
        average_price_per_state = df.groupby('estado')['preco'].mean()
        if average_price_per_state.empty:
            print("  No price data available for calculating averages per state.")
        else:
            for state, avg_price in average_price_per_state.items():
                print(f"  {state}: {format_currency(avg_price)}")
    else:
        # This case should ideally be caught earlier, but as a safeguard:
        print("  'preco' column is missing, cannot calculate average prices.")
    print("--------------------------------------------------")

    # Geocoding Statistics
    print("Geocoding Statistics:")
    print("--------------------------------------------------")
    if 'latitude' not in df.columns or 'longitude' not in df.columns:
        print("  Latitude/Longitude columns not found. Geocoding statistics cannot be generated.")
    else:
        total_geocoded = df['latitude'].notna().sum()
        percentage_geocoded_overall = (total_geocoded / total_properties) * 100 if total_properties > 0 else 0
        
        print(f"Overall geocoding success rate: {percentage_geocoded_overall:.1f}% ({total_geocoded} out of {total_properties} properties)")
        print("\nGeocoding success rate per state:")

        geocoded_per_state_counts = df.groupby('estado')['latitude'].count() # Counts non-NaN latitudes
        
        # properties_per_state is already df.groupby('estado').size()
        
        # Combine total properties per state with geocoded counts
        state_stats_df = pd.DataFrame({
            'total': properties_per_state,
            'geocoded': geocoded_per_state_counts
        }).fillna(0) # Fill NaN for states with 0 properties or 0 geocoded properties in the .count() series
        
        state_stats_df['percentage'] = (state_stats_df['geocoded'] / state_stats_df['total'] * 100).fillna(0)

        if state_stats_df.empty:
            print("  No per-state geocoding data available.")
        else:
            for state, row in state_stats_df.iterrows():
                print(f"  {state}: {row['percentage']:.1f}% ({int(row['geocoded'])} out of {int(row['total'])} properties)")
    print("--------------------------------------------------")

def main():
    # You could use argparse here to take filepath from command line
    # For simplicity, we'll use the default or a fixed one if needed for main execution.
    generate_report() # This will use "imoveis_BR.csv" by default

if __name__ == "__main__":
    # Example: To run from command line for a different file:
    # python reporter.py path/to/your/file.csv
    # For now, main() calls generate_report() without arguments, using the default.
    # If you want to parse command line arguments:
    # import sys
    # if len(sys.argv) > 1:
    #     generate_report(sys.argv[1])
    # else:
    #     main()
    main()
