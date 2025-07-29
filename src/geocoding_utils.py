# geocoding_utils.py
import sqlite3
from pathlib import Path
import time

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError

# --- SQLite Cache Setup ---
DB_NAME = "cache.sqlite"
TABLE_NAME = "coords"

def _init_cache_db():
    """Initializes the SQLite database and coords table if they don't exist."""
    db_path = Path(DB_NAME)
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        address TEXT PRIMARY KEY,
        lat REAL,
        lon REAL
    )
    """)
    conn.commit()
    conn.close()

_init_cache_db() # Initialize DB when module is loaded

def _get_cached_coords(address: str) -> tuple | None:
    """Retrieves coordinates from cache. Returns (lat, lon) or None if not found."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(f"SELECT lat, lon FROM {TABLE_NAME} WHERE address = ?", (address,))
    result = cursor.fetchone()
    conn.close()
    return result if result else None

def _cache_coords(address: str, lat: float, lon: float):
    """Stores coordinates in cache."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute(f"INSERT INTO {TABLE_NAME} (address, lat, lon) VALUES (?, ?, ?)", (address, lat, lon))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()
# --- End SQLite Cache Setup ---

_geolocators_cache = {}
_default_user_agent = "caixaaberta_geocoder/1.1"

def get_coordinates_for_address(address_str: str, api_key: str = None):
    """
    Geocodes a given address string to latitude and longitude using Nominatim.
    """
    user_agent = api_key if api_key else _default_user_agent

    if user_agent not in _geolocators_cache:
        print(f"Initializing Nominatim with User-Agent: {user_agent}")
        _geolocators_cache[user_agent] = Nominatim(user_agent=user_agent)

    geolocator = _geolocators_cache[user_agent]

    if not address_str or not isinstance(address_str, str) or address_str.strip() == "":
        return (None, None)

    cached_coords = _get_cached_coords(address_str)
    if cached_coords:
        return cached_coords

    try:
        time.sleep(1) # Manual rate limiting
        location = geolocator.geocode(address_str, timeout=10)

        if location and hasattr(location, 'latitude') and hasattr(location, 'longitude'):
            lat, lon = location.latitude, location.longitude
            _cache_coords(address_str, lat, lon)
            return (lat, lon)
        else:
            return (None, None)

    except GeocoderTimedOut:
        print(f"Warning: Geocoding timed out for address '{address_str}'.")
        return (None, None)
    except GeocoderUnavailable:
        print(f"Warning: Geocoding service (Nominatim) unavailable for address '{address_str}'.")
        return (None, None)
    except GeocoderServiceError as e:
        print(f"Warning: Geocoding service error for address '{address_str}': {e}.")
        return (None, None)
    except Exception as e:
        print(f"Warning: An unexpected error occurred during geocoding for address '{address_str}': {e}.")
        return (None, None)

if __name__ == '__main__':
    test_addresses = [
        "Praça da Sé, São Paulo, SP",
        "Rua XYZ, 99999, Cidade Inexistente, XX",
        "1600 Amphitheatre Parkway, Mountain View, CA",
        "",
        None,
        "   ",
        "Torre Eiffel, Paris, França"
    ]

    print("Starting geocoding tests:")
    for i, addr in enumerate(test_addresses):
        print(f"\nTest {i+1}: Geocoding address: '{addr}'")
        lat, lon = get_coordinates_for_address(addr)
        if lat is not None and lon is not None:
            print(f"  -> Coordinates: ({lat}, {lon})")
        else:
            print("  -> Could not retrieve coordinates.")
    print("\nGeocoding tests finished.")
