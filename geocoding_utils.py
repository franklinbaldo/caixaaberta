# geocoding_utils.py

from geopy.geocoders import Nominatim
from geopy.extra.adapters import RateLimiter
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError

# Initialize Nominatim geocoder with a custom user-agent
# This is required by Nominatim's usage policy.
_geolocator = Nominatim(user_agent="caixaaberta_geocoder/1.0")

# Wrap the geocoder instance with RateLimiter to respect usage policies (1 request per second)
_geocode_with_limiter = RateLimiter(_geolocator.geocode, min_delay_seconds=1)

def get_coordinates_for_address(address_str: str):
    """
    Geocodes a given address string to latitude and longitude using Nominatim.

    The function handles rate limiting (1 request per second) and includes error
    handling for common geocoding issues.

    Args:
        address_str (str): The address string to geocode (e.g., "Rua Exemplo, 123, Cidade, Estado").

    Returns:
        tuple: A tuple containing (latitude, longitude) if the address is successfully
               geocoded, or (None, None) if the address cannot be found, if the input
               is invalid, or if any geocoding error occurs.
    """
    if not address_str or not isinstance(address_str, str) or address_str.strip() == "":
        # print(f"Warning: Invalid or empty address string provided: '{address_str}'") # Optional logging
        return (None, None)

    try:
        location = _geocode_with_limiter(address_str, timeout=10)
        
        if location and hasattr(location, 'latitude') and hasattr(location, 'longitude'):
            return (location.latitude, location.longitude)
        else:
            # Address was processed by Nominatim but no specific location point was found.
            # print(f"Warning: Geocoding for address '{address_str}' did not return a valid location.") # Optional
            return (None, None)
            
    except GeocoderTimedOut:
        print(f"Warning: Geocoding timed out for address '{address_str}'.")
        return (None, None)
    except GeocoderUnavailable:
        print(f"Warning: Geocoding service (Nominatim) unavailable for address '{address_str}'.")
        return (None, None)
    except GeocoderServiceError as e:
        # This can catch more specific service errors like 5xx from Nominatim
        print(f"Warning: Geocoding service error for address '{address_str}': {e}")
        return (None, None)
    except Exception as e:
        # Catch any other unexpected errors during geocoding
        print(f"Warning: An unexpected error occurred during geocoding for address '{address_str}': {e}")
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
