# geocoding_utils.py
import sqlite3
from pathlib import Path

from geopy.geocoders import Nominatim
from geopy.extra.adapters import RateLimiter # Reverted to original import path
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError

# --- SQLite Cache Setup ---
DB_NAME = "cache.sqlite"
TABLE_NAME = "coords"

def _init_cache_db():
    """Initializes the SQLite database and coords table if they don't exist."""
    db_path = Path(DB_NAME)
    # No need to check db_path.exists(), connect will create it if not present.
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
    except sqlite3.IntegrityError: # Address already exists, which is fine.
        pass # Or could update, but for this use case, first entry is likely fine.
    finally:
        conn.close()
# --- End SQLite Cache Setup ---


# Initialize Nominatim geocoder with a custom user-agent
# This is required by Nominatim's usage policy.
# _geolocator will be initialized within the function or globally with a default.

# Global store for initialized geolocators to avoid re-initialization with the same user_agent
_geolocators_cache = {}
_default_user_agent = "caixaaberta_geocoder/1.1" # Updated default

def get_coordinates_for_address(address_str: str, api_key: str = None):
    """
    Geocodes a given address string to latitude and longitude using Nominatim.

    The function handles rate limiting (1 request per second) and includes error
    handling for common geocoding issues. It uses the provided api_key as the
    User-Agent for Nominatim, or a default one if not provided.

    Args:
        address_str (str): The address string to geocode.
        api_key (str, optional): The User-Agent string (e.g., email for Nominatim)
                                 to use for the geocoding request. Defaults to None,
                                 which uses a default User-Agent.

    Returns:
        tuple: A tuple containing (latitude, longitude) if successfully geocoded,
               or (None, None) otherwise.
    """
    user_agent = api_key if api_key else _default_user_agent

    if user_agent not in _geolocators_cache:
        print(f"Initializing Nominatim with User-Agent: {user_agent}")
        geolocator_instance = Nominatim(user_agent=user_agent)
        _geolocators_cache[user_agent] = RateLimiter(geolocator_instance.geocode, min_delay_seconds=1)

    geocode_with_limiter = _geolocators_cache[user_agent]

    if not address_str or not isinstance(address_str, str) or address_str.strip() == "":
        return (None, None)

    # 1. Check cache first
    cached_coords = _get_cached_coords(address_str)
    if cached_coords:
        # print(f"Cache hit for '{address_str}': {cached_coords}") # Optional: for debugging
        return cached_coords

    # 2. If not in cache, proceed with geocoding
    # print(f"Cache miss for '{address_str}'. Geocoding...") # Optional: for debugging
    try:
        location = geocode_with_limiter(address_str, timeout=10)

        if location and hasattr(location, 'latitude') and hasattr(location, 'longitude'):
            lat, lon = location.latitude, location.longitude
            # 3. Cache the new result
            _cache_coords(address_str, lat, lon)
            return (lat, lon)
        else:
            # Address was processed by Nominatim but no specific location point was found.
            # Do not cache non-results to allow for future retries if the service improves
            # or if the address becomes geocodable later.
            return (None, None)

    except GeocoderTimedOut:
        print(f"Warning: Geocoding timed out for address '{address_str}'. Not caching.")
        return (None, None)
    except GeocoderUnavailable:
        print(f"Warning: Geocoding service (Nominatim) unavailable for address '{address_str}'. Not caching.")
        return (None, None)
    except GeocoderServiceError as e:
        print(f"Warning: Geocoding service error for address '{address_str}': {e}. Not caching.")
        return (None, None)
    except Exception as e:
        print(f"Warning: An unexpected error occurred during geocoding for address '{address_str}': {e}. Not caching.")
        return (None, None)

if __name__ == '__main__':
    # Example Usage (for testing purposes)
    # Note: Running this directly will make actual calls to Nominatim and is rate-limited.
    test_addresses = [
        "Praça da Sé, São Paulo, SP", # Valid
        "Rua XYZ, 99999, Cidade Inexistente, XX", # Likely invalid
        "1600 Amphitheatre Parkway, Mountain View, CA", # Valid (testing non-Brazilian)
        "", # Empty
        None, # None
        "   ", # Whitespace only
        "Torre Eiffel, Paris, França" # Valid
    ]

    print("Starting geocoding tests (will be rate-limited to 1 per second):")
    for i, addr in enumerate(test_addresses):
        print(f"\nTest {i+1}: Geocoding address: '{addr}'")
        lat, lon = get_coordinates_for_address(addr)
        if lat is not None and lon is not None:
            print(f"  -> Coordinates: ({lat}, {lon})")
        else:
            print(f"  -> Could not retrieve coordinates.")
    print("\nGeocoding tests finished.")
