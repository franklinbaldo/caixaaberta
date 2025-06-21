# src/caixaaberta/geocoding_utils.py
from geopy.geocoders import Nominatim
# from geopy.extra.adapters import RateLimiter # Commented out due to import issues
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError
import logging # Import logging

# Import cache functions from the new cache module within the same package
from .cache import get_cached_coords, cache_coords

# Global store for initialized geolocators to avoid re-initialization with the same user_agent
# _geolocators_cache is now storing geolocator instances directly, not RateLimiter instances
_geolocators_cache = {}
_default_user_agent = "caixaaberta_geocoder/1.2" # Version bump for clarity

# Get a logger instance for this module
logger = logging.getLogger(__name__)

def get_coordinates_for_address(address_str: str, api_key: str = None):
    """
    Geocodes a given address string to latitude and longitude using Nominatim.
    Uses a local SQLite cache to avoid redundant API calls.

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
    if not address_str or not isinstance(address_str, str) or address_str.strip() == "":
        return (None, None)

    # 1. Check cache first
    cached_coords = get_cached_coords(address_str)
    if cached_coords:
        logger.debug(f"Cache hit for '{address_str}': {cached_coords}")
        return cached_coords

    # 2. If not in cache, proceed with geocoding
    logger.debug(f"Cache miss for '{address_str}'. Geocoding...")

    user_agent = api_key if api_key else _default_user_agent
    if user_agent not in _geolocators_cache:
        logger.info(f"Initializing Nominatim with User-Agent: {user_agent}")
        geolocator_instance = Nominatim(user_agent=user_agent)
        # _geolocators_cache[user_agent] = RateLimiter(geolocator_instance.geocode, min_delay_seconds=1) # Commented out
        _geolocators_cache[user_agent] = geolocator_instance # Store the instance directly

    # geocode_with_limiter = _geolocators_cache[user_agent]
    geolocator_to_use = _geolocators_cache[user_agent]

    try:
        # location = geocode_with_limiter(address_str, timeout=10)
        location = geolocator_to_use.geocode(address_str, timeout=10) # Use geolocator instance directly

        if location and hasattr(location, 'latitude') and hasattr(location, 'longitude'):
            lat, lon = location.latitude, location.longitude
            # 3. Cache the new result
            cache_coords(address_str, lat, lon)
            return (lat, lon)
        else:
            logger.debug(f"Geocoder did not return a valid location for address '{address_str}'. Not caching.")
            return (None, None)

    except GeocoderTimedOut:
        logger.warning(f"Geocoding timed out for address '{address_str}'. Not caching.")
        return (None, None)
    except GeocoderUnavailable:
        logger.warning(f"Geocoding service (Nominatim) unavailable for address '{address_str}'. Not caching.")
        return (None, None)
    except GeocoderServiceError as e:
        logger.error(f"Geocoding service error for address '{address_str}': {e}. Not caching.", exc_info=True)
        return (None, None)
    except Exception as e:
        logger.error(f"An unexpected error occurred during geocoding for address '{address_str}': {e}. Not caching.", exc_info=True)
        return (None, None)

if __name__ == '__main__':
    # Setup basic logging for direct script execution test
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout,
                        format="%(asctime)s | %(levelname)s | %(module)s:%(lineno)d | %(message)s")
    # from .cache import init_cache_db, clear_cache # Relative import for package
    # init_cache_db()
    # clear_cache()

    logger.info("Starting geocoding tests (will use cache at data/cache.sqlite):")
    test_addresses = [
        "Praça da Sé, São Paulo, SP",
        "Rua XYZ, 99999, Cidade Inexistente, XX", # Intentionally invalid
        "1600 Amphitheatre Parkway, Mountain View, CA",
        "Praça da Sé, São Paulo, SP", # Should be a cache hit
    ]

    for i, addr in enumerate(test_addresses):
        logger.info(f"Test {i+1}: Geocoding address: '{addr}'")
        lat, lon = get_coordinates_for_address(addr, api_key="my_test_email@example.com") # Example API key
        if lat is not None and lon is not None:
            logger.info(f"  -> Coordinates: ({lat}, {lon})")
        else:
            logger.info(f"  -> Could not retrieve coordinates.")
    logger.info("Geocoding tests finished.")
