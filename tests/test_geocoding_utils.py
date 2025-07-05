import pytest
import time
from unittest.mock import patch, MagicMock

import sqlite3
from pathlib import Path
from unittest.mock import patch, MagicMock, ANY

# Import the functions to be tested
from src.geocoding_utils import get_coordinates_for_address, _init_cache_db, _get_cached_coords, _cache_coords

# Test Nominatim User-Agent and DB Name
TEST_USER_AGENT = "caixaaberta_test_suite/1.0"
TEST_DB_NAME = "test_cache.sqlite" # Use a separate DB for tests
TABLE_NAME = "coords" # from geocoding_utils

@pytest.fixture(scope="function", autouse=True)
def manage_test_db():
    """
    Fixture to set up a test database before each test function and tear it down after.
    It patches DB_NAME in geocoding_utils to use TEST_DB_NAME.
    """
    with patch('src.geocoding_utils.DB_NAME', TEST_DB_NAME):
        # Ensure a clean state by deleting the test DB if it exists from a previous run
        db_path = Path(TEST_DB_NAME)
        if db_path.exists():
            db_path.unlink()

        # Initialize the test database
        _init_cache_db() # This will now use TEST_DB_NAME

        yield # Test runs here

        # Teardown: delete the test database file
        if db_path.exists():
            db_path.unlink()

@pytest.fixture
def mock_nominatim_success():
    """Mocks a successful response from Nominatim and the RateLimiter."""
    mock_location.latitude = 12.345
    mock_location.longitude = -67.890
    mock_location.address = "Test Address, City, Country" # For debugging if needed

    # This is the geocode function that RateLimiter would wrap
    mock_geocode_function = MagicMock(return_value=mock_location)

    # Mock the RateLimiter to directly call our mock_geocode_function
    # or return its result without actual rate limiting logic for tests.
    mock_rate_limited_geocode = MagicMock(side_effect=lambda address, timeout: mock_geocode_function(address, timeout=timeout))


    # We need to patch 'Nominatim' to return a mock geolocator instance,
    # and then ensure that RateLimiter is called with this instance's geocode method.
    # The RateLimiter's __call__ (which is what geocode_with_limiter becomes)
    # should then use our mock_geocode_function.

    # Patching strategy:
    # 1. Patch Nominatim constructor: `patch('geopy.geocoders.Nominatim', ...)`
    #    - Its return_value (the geolocator instance) should have a `geocode` method.
    # 2. Patch RateLimiter constructor: `patch('geopy.extra.adapters.RateLimiter', ...)`
    #    - Its return_value (the rate limited geocoder) should be our `mock_rate_limited_geocode`.

    # For simplicity in the fixture, we'll assume the test will handle the patching
    # of Nominatim and RateLimiter, and this fixture provides the end result of geocoding.
    # The test itself will then assert that Nominatim and RateLimiter were configured as expected.
    return mock_geocode_function, mock_rate_limited_geocode


@pytest.fixture
def mock_nominatim_failure_none():
    """Mocks a failure (None response) from Nominatim via RateLimiter."""
    mock_geocode_function = MagicMock(return_value=None)
    mock_rate_limited_geocode = MagicMock(side_effect=lambda address, timeout: mock_geocode_function(address, timeout=timeout))
    return mock_geocode_function, mock_rate_limited_geocode

@pytest.fixture
def mock_nominatim_exception():
    """Mocks an exception from Nominatim via RateLimiter."""
    from geopy.exc import GeocoderServiceError # More specific error
    mock_geocode_function = MagicMock(side_effect=GeocoderServiceError("Simulated geocoding service error"))
    mock_rate_limited_geocode = MagicMock(side_effect=lambda address, timeout: mock_geocode_function(address, timeout=timeout))
    return mock_geocode_function, mock_rate_limited_geocode


def test_get_coordinates_successful_geocoding_no_cache(mock_nominatim_success):
    """Test successful geocoding when the address is not in cache."""
    raw_geocode_mock, rate_limited_geocode_mock = mock_nominatim_success

    # Mock Nominatim constructor
    mock_geolocator_instance = MagicMock()
    mock_geolocator_instance.geocode = raw_geocode_mock # The method RateLimiter will wrap

    with patch('geopy.geocoders.Nominatim', return_value=mock_geolocator_instance) as mock_nominatim_constructor, \
         patch('geopy.extra.adapters.RateLimiter', return_value=rate_limited_geocode_mock) as mock_rate_limiter_constructor:

        lat, lon = get_coordinates_for_address("Test Address 1", api_key=TEST_USER_AGENT)

        assert lat == 12.345
        assert lon == -67.890
        mock_nominatim_constructor.assert_called_once_with(user_agent=TEST_USER_AGENT)
        # RateLimiter should be initialized with the geocode method of the Nominatim instance
        mock_rate_limiter_constructor.assert_called_once_with(mock_geolocator_instance.geocode, min_delay_seconds=1)
        # The rate_limited_geocode (which simulates RateLimiter.__call__) should be called
        rate_limited_geocode_mock.assert_called_once_with("Test Address 1", timeout=10)
        # Check that the underlying (raw) geocode function was also called by the rate limiter mock
        raw_geocode_mock.assert_called_once_with("Test Address 1", timeout=10)

        # Verify cache write
        cached_val = _get_cached_coords("Test Address 1")
        assert cached_val is not None
        assert cached_val[0] == 12.345
        assert cached_val[1] == -67.890

def test_get_coordinates_successful_geocoding_with_cache():
    """Test successful geocoding when the address is already in cache (no external call)."""
    # Pre-populate cache (using TEST_DB_NAME implicitly via manage_test_db fixture)
    _cache_coords("Test Address Cached", 54.321, -98.765)

    # We don't need mock_nominatim_success here as it shouldn't be called.
    # We patch Nominatim and RateLimiter to ensure they are NOT called.
    with patch('geopy.geocoders.Nominatim') as mock_nominatim_constructor, \
         patch('geopy.extra.adapters.RateLimiter') as mock_rate_limiter_constructor:

        lat, lon = get_coordinates_for_address("Test Address Cached", api_key=TEST_USER_AGENT)

        assert lat == 54.321
        assert lon == -98.765
        mock_nominatim_constructor.assert_not_called()
        mock_rate_limiter_constructor.assert_not_called()


def test_get_coordinates_failure_none_response_no_cache(mock_nominatim_failure_none):
    """Test geocoding failure (None response) when address is not in cache."""
    raw_geocode_mock, rate_limited_geocode_mock = mock_nominatim_failure_none
    mock_geolocator_instance = MagicMock()
    mock_geolocator_instance.geocode = raw_geocode_mock

    with patch('geopy.geocoders.Nominatim', return_value=mock_geolocator_instance) as mock_nominatim_constructor, \
         patch('geopy.extra.adapters.RateLimiter', return_value=rate_limited_geocode_mock) as mock_rate_limiter_constructor:

        lat, lon = get_coordinates_for_address("Unknown Address 1", api_key=TEST_USER_AGENT)

        assert lat is None
        assert lon is None
        mock_nominatim_constructor.assert_called_once_with(user_agent=TEST_USER_AGENT)
        mock_rate_limiter_constructor.assert_called_once_with(mock_geolocator_instance.geocode, min_delay_seconds=1)
        rate_limited_geocode_mock.assert_called_once_with("Unknown Address 1", timeout=10)
        raw_geocode_mock.assert_called_once_with("Unknown Address 1", timeout=10)

        # Current implementation does NOT cache None results for failures, to allow retries.
        # So, _get_cached_coords should return None.
        cached_val = _get_cached_coords("Unknown Address 1")
        assert cached_val is None

def test_get_coordinates_failure_exception_no_cache(mock_nominatim_exception):
    """Test geocoding failure (exception) when address is not in cache."""
    raw_geocode_mock, rate_limited_geocode_mock = mock_nominatim_exception
    mock_geolocator_instance = MagicMock()
    mock_geolocator_instance.geocode = raw_geocode_mock

    with patch('geopy.geocoders.Nominatim', return_value=mock_geolocator_instance) as mock_nominatim_constructor, \
         patch('geopy.extra.adapters.RateLimiter', return_value=rate_limited_geocode_mock) as mock_rate_limiter_constructor:

        lat, lon = get_coordinates_for_address("Exception Address 1", api_key=TEST_USER_AGENT)

        assert lat is None
        assert lon is None
        mock_nominatim_constructor.assert_called_once_with(user_agent=TEST_USER_AGENT)
        mock_rate_limiter_constructor.assert_called_once_with(mock_geolocator_instance.geocode, min_delay_seconds=1)
        rate_limited_geocode_mock.assert_called_once_with("Exception Address 1", timeout=10)
        raw_geocode_mock.assert_called_once_with("Exception Address 1", timeout=10)

        # Exceptions also should not lead to caching a "None" result.
        cached_val = _get_cached_coords("Exception Address 1")
        assert cached_val is None


def test_get_coordinates_empty_or_invalid_address():
    """Test that empty, None, or whitespace-only address returns (None, None) without calling Nominatim."""
    with patch('geopy.geocoders.Nominatim') as mock_nominatim_constructor, \
         patch('geopy.extra.adapters.RateLimiter') as mock_rate_limiter_constructor:

        invalid_addresses = [None, "", "   ", 12345] # Added non-string type
        for addr in invalid_addresses:
            lat, lon = get_coordinates_for_address(addr, api_key=TEST_USER_AGENT)
            assert lat is None
            assert lon is None
            # Ensure cache is not populated for these invalid inputs
            if isinstance(addr, str): # Only string keys are attempted for caching
                 cached_val = _get_cached_coords(addr)
                 assert cached_val is None


        mock_nominatim_constructor.assert_not_called()
        mock_rate_limiter_constructor.assert_not_called()


def test_api_key_usage_for_nominatim():
    """Test that the api_key (user_agent) is correctly passed to Nominatim."""
    # This test focuses on the user_agent passed to Nominatim constructor.
    # It uses a simplified mock for the geocoding part.
    mock_geocode_func = MagicMock(return_value=None) # Irrelevant for this test
    mock_rate_limited_func = MagicMock(side_effect=lambda address, timeout: mock_geocode_func(address, timeout=timeout))

    mock_geolocator_instance = MagicMock()
    mock_geolocator_instance.geocode = mock_geocode_func

    with patch('geopy.geocoders.Nominatim', return_value=mock_geolocator_instance) as mock_constructor, \
         patch('geopy.extra.adapters.RateLimiter', return_value=mock_rate_limited_func): # Patch RateLimiter too

        get_coordinates_for_address("Some Address", api_key="my_custom_user_agent")
        mock_constructor.assert_called_once_with(user_agent="my_custom_user_agent")

    # Test with default user_agent if api_key is None
    # Need to clear the geolocators_cache in geocoding_utils as it stores based on user_agent
    with patch('src.geocoding_utils._geolocators_cache', {}), \
         patch('geopy.geocoders.Nominatim', return_value=mock_geolocator_instance) as mock_constructor, \
         patch('geopy.extra.adapters.RateLimiter', return_value=mock_rate_limited_func):
        from src.geocoding_utils import _default_user_agent # Import default to check against
        get_coordinates_for_address("Some Other Address", api_key=None)
        mock_constructor.assert_called_once_with(user_agent=_default_user_agent)


def test_cache_behavior_sqlite():
    """Test direct SQLite cache functions _cache_coords and _get_cached_coords."""
    address1 = "123 Main St"
    lat1, lon1 = 34.0522, -118.2437
    _cache_coords(address1, lat1, lon1)

    cached = _get_cached_coords(address1)
    assert cached is not None
    assert cached[0] == lat1
    assert cached[1] == lon1

    # Test fetching non-existent address
    cached_non_existent = _get_cached_coords("Non Existent Address")
    assert cached_non_existent is None

    # Test overwriting (or rather, ignoring due to PRIMARY KEY constraint if address exists)
    # For this test, let's assume INSERT OR IGNORE or similar behavior.
    # If an integrity error occurs, it means the first entry is kept.
    _cache_coords(address1, 99.9, -99.9) # Attempt to cache different coords for same address
    cached_after_reattempt = _get_cached_coords(address1)
    assert cached_after_reattempt[0] == lat1 # Should still be the original lat1
    assert cached_after_reattempt[1] == lon1 # Should still be the original lon1

    # Verify with a direct DB query if necessary for more complex scenarios,
    # but for unit testing _get_cached_coords, this should suffice.
    conn = sqlite3.connect(TEST_DB_NAME)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE address = ?", (address1,))
    count = cursor.fetchone()[0]
    conn.close()
    assert count == 1 # Ensure no duplicate entries


# Note: The original test_cache_expiry and test_clear_geocode_cache_functionality
# were based on an in-memory dictionary cache (_geocode_cache).
# Since the implementation now uses SQLite, these tests need to be adapted or removed
# if cache expiry is not a feature of the SQLite cache or if clearing is handled differently.
# The current SQLite cache in geocoding_utils.py does not have an explicit expiry mechanism.
# Clearing the cache would mean deleting the DB file or records, which manage_test_db handles per test.

# If a global "clear all cache" function for SQLite (like deleting all rows) was added to geocoding_utils,
# it could be tested here. For now, manage_test_db ensures test isolation.
