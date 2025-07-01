import pytest
import time
from unittest.mock import patch, MagicMock

# Import the functions to be tested
# Assuming geocoding_utils.py is in src and tests are in tests/
# Ensure src is in PYTHONPATH or adjust import as necessary
from geocoding_utils import get_coordinates_for_address, clear_geocode_cache, _geocode_cache

# Test Nominatim User-Agent
NOMINATIM_USER_AGENT = "real-estate-pipeline-test"

@pytest.fixture(autouse=True)
def clear_cache_before_each_test():
    """Fixture to clear the cache before each test."""
    clear_geocode_cache()
    # Also, ensure the cache is empty at the start, useful if tests run out of order or are interrupted
    _geocode_cache.clear()

@pytest.fixture
def mock_nominatim_success():
    """Mocks a successful response from Nominatim."""
    mock_location = MagicMock()
    mock_location.latitude = 12.345
    mock_location.longitude = -67.890
    mock_location.address = "Test Address, City, Country"

    mock_geolocator = MagicMock()
    mock_geolocator.geocode.return_value = mock_location
    return mock_geolocator

@pytest.fixture
def mock_nominatim_failure_none():
    """Mocks a failure (None response) from Nominatim."""
    mock_geolocator = MagicMock()
    mock_geolocator.geocode.return_value = None
    return mock_geolocator

@pytest.fixture
def mock_nominatim_exception():
    """Mocks an exception from Nominatim."""
    mock_geolocator = MagicMock()
    # Simulate a GeopyServiceError or similar timeout/exception
    from geopy.exc import GeopyError
    mock_geolocator.geocode.side_effect = GeopyError("Simulated geocoding error")
    return mock_geolocator


def test_get_coordinates_successful_geocoding_no_cache(mock_nominatim_success):
    """Test successful geocoding when the address is not in cache."""
    with patch('geopy.geocoders.Nominatim', return_value=mock_nominatim_success) as mock_geolocator_constructor:
        # Pass api_key (user_agent for Nominatim)
        lat, lon = get_coordinates_for_address("Test Address 1", api_key=NOMINATIM_USER_AGENT)

        assert lat == 12.345
        assert lon == -67.890
        mock_geolocator_constructor.assert_called_once_with(user_agent=NOMINATIM_USER_AGENT, timeout=10)
        mock_nominatim_success.geocode.assert_called_once_with("Test Address 1")
        assert "Test Address 1" in _geocode_cache # Check if address was added to cache

def test_get_coordinates_successful_geocoding_with_cache(mock_nominatim_success):
    """Test successful geocoding when the address is already in cache."""
    # Pre-populate cache
    _geocode_cache["Test Address Cached"] = (54.321, -98.765, time.time())

    with patch('geopy.geocoders.Nominatim', return_value=mock_nominatim_success) as mock_geolocator_constructor:
        lat, lon = get_coordinates_for_address("Test Address Cached", api_key=NOMINATIM_USER_AGENT)

        assert lat == 54.321
        assert lon == -98.765
        mock_geolocator_constructor.assert_not_called() # Nominatim should not be called
        mock_nominatim_success.geocode.assert_not_called() # geocode method should not be called

def test_get_coordinates_failure_none_response_no_cache(mock_nominatim_failure_none):
    """Test geocoding failure (None response) when address is not in cache."""
    with patch('geopy.geocoders.Nominatim', return_value=mock_nominatim_failure_none) as mock_geolocator_constructor:
        lat, lon = get_coordinates_for_address("Unknown Address 1", api_key=NOMINATIM_USER_AGENT)

        assert lat is None
        assert lon is None
        mock_geolocator_constructor.assert_called_once_with(user_agent=NOMINATIM_USER_AGENT, timeout=10)
        mock_nominatim_failure_none.geocode.assert_called_once_with("Unknown Address 1")
        # Cache should store None for failures to prevent re-querying too soon (if desired, current impl does)
        assert "Unknown Address 1" in _geocode_cache
        assert _geocode_cache["Unknown Address 1"][0] is None
        assert _geocode_cache["Unknown Address 1"][1] is None

def test_get_coordinates_failure_exception_no_cache(mock_nominatim_exception):
    """Test geocoding failure (exception) when address is not in cache."""
    with patch('geopy.geocoders.Nominatim', return_value=mock_nominatim_exception) as mock_geolocator_constructor:
        lat, lon = get_coordinates_for_address("Exception Address 1", api_key=NOMINATIM_USER_AGENT)

        assert lat is None
        assert lon is None
        mock_geolocator_constructor.assert_called_once_with(user_agent=NOMINATIM_USER_AGENT, timeout=10)
        mock_nominatim_exception.geocode.assert_called_once_with("Exception Address 1")
        # Cache should also store None for failures due to exception
        assert "Exception Address 1" in _geocode_cache
        assert _geocode_cache["Exception Address 1"][0] is None
        assert _geocode_cache["Exception Address 1"][1] is None


def test_get_coordinates_empty_or_none_address():
    """Test that empty or None address returns (None, None) without calling Nominatim."""
    with patch('geopy.geocoders.Nominatim') as mock_geolocator_constructor:
        # Test with None
        lat, lon = get_coordinates_for_address(None, api_key=NOMINATIM_USER_AGENT)
        assert lat is None
        assert lon is None

        # Test with empty string
        lat, lon = get_coordinates_for_address("", api_key=NOMINATIM_USER_AGENT)
        assert lat is None
        assert lon is None

        # Test with whitespace-only string
        lat, lon = get_coordinates_for_address("   ", api_key=NOMINATIM_USER_AGENT)
        assert lat is None
        assert lon is None

        mock_geolocator_constructor.assert_not_called() # Nominatim should not be initialized

def test_cache_expiry():
    """Test that cache entries expire after the specified CACHE_EXPIRY_SECONDS."""
    with patch('geopy.geocoders.Nominatim') as mock_geolocator_constructor:
        # Mock successful geocode call
        mock_location = MagicMock()
        mock_location.latitude = 11.111
        mock_location.longitude = 22.222
        mock_geolocator_constructor.return_value.geocode.return_value = mock_location

        # Geocode an address to cache it
        get_coordinates_for_address("AddressToExpire", api_key=NOMINATIM_USER_AGENT, cache_expiry_seconds=0.1)
        mock_geolocator_constructor.return_value.geocode.assert_called_once_with("AddressToExpire")

        # Wait for longer than cache expiry
        time.sleep(0.2)

        # Reset mock for the second call
        mock_geolocator_constructor.return_value.geocode.reset_mock()

        # Geocode the same address again
        get_coordinates_for_address("AddressToExpire", api_key=NOMINATIM_USER_AGENT, cache_expiry_seconds=0.1)

        # Check that geocode was called again (cache expired)
        mock_geolocator_constructor.return_value.geocode.assert_called_once_with("AddressToExpire")

def test_clear_geocode_cache_functionality():
    """Test that clear_geocode_cache actually clears the cache."""
    _geocode_cache["Test Address to Clear"] = (1.0, 1.0, time.time())
    assert "Test Address to Clear" in _geocode_cache

    clear_geocode_cache()
    assert "Test Address to Clear" not in _geocode_cache
    assert not _geocode_cache # Cache should be empty

def test_api_key_usage_for_nominatim():
    """Test that the api_key (user_agent) is correctly passed to Nominatim."""
    mock_geolocator = MagicMock()
    mock_geolocator.geocode.return_value = None # Don't care about the result for this test

    with patch('geopy.geocoders.Nominatim', return_value=mock_geolocator) as mock_constructor:
        get_coordinates_for_address("Some Address", api_key="my_custom_user_agent")
        mock_constructor.assert_called_once_with(user_agent="my_custom_user_agent", timeout=10)

    clear_geocode_cache() # Clear after this test as it uses a different user_agent

    # Test with default user_agent if api_key is None
    with patch('geopy.geocoders.Nominatim', return_value=mock_geolocator) as mock_constructor:
        from geocoding_utils import DEFAULT_USER_AGENT # Import default to check against
        get_coordinates_for_address("Some Other Address", api_key=None)
        mock_constructor.assert_called_once_with(user_agent=DEFAULT_USER_AGENT, timeout=10)

# It might be useful to also test the behavior when GEOCODER_KEY_ENV_VAR is used,
# but that involves mocking os.getenv, which can be a bit more involved if not careful.
# For now, testing direct api_key passthrough covers the core logic of key handling by the function.

# If geocoding_utils.py uses other geocoders via a factory or conditional logic based on api_key,
# those paths would need separate tests and mocks. Current version seems to be Nominatim-focused.
# Example:
# @patch.dict(os.environ, {"GEOCODER_KEY": "env_user_agent"})
# def test_geocoding_with_env_var_key(mock_nominatim_success):
# ... etc ...
