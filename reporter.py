# reporter.py
import pandas as pd
import locale

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

def generate_report(csv_filepath: str = "imoveis_BR.csv"):
    """
    Loads data from the specified CSV file, calculates summary statistics,
    and prints them to the console.

    Args:
        csv_filepath (str): The path to the CSV file to report on.
                            Defaults to "imoveis_BR.csv".
    """
    try:
        df = pd.read_csv(csv_filepath, dtype={'estado': str})
        # Ensure 'preco' is float, handling potential errors during conversion
        if 'preco' in df.columns:
            df['preco'] = pd.to_numeric(df['preco'], errors='coerce')
        else:
            print(f"Error: 'preco' column is missing from {csv_filepath}.")
            return

    except FileNotFoundError:
        print(f"Error: {csv_filepath} not found.")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: {csv_filepath} is empty or malformed.")
        return
    except Exception as e:
        print(f"An unexpected error occurred while loading the CSV {csv_filepath}: {e}")
        return

    if df.empty:
        print(f"{csv_filepath} is empty. No statistics to generate.")
        return

    # Check for essential columns
    if 'estado' not in df.columns:
        print(f"Error: 'estado' column is missing from {csv_filepath}.")
        return
    # 'preco' presence is already checked and handled during loading

    print(f"Real Estate Data Report for {csv_filepath}:")
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
