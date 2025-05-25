import unittest
from unittest.mock import patch, MagicMock, call
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable

# Assuming geocoding_utils.py is in the parent directory or PYTHONPATH is set.
# If geocoding_utils.py is in the root, and tests is a subdir:
import sys
import os
# Add the root directory to sys.path to allow direct import of geocoding_utils
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from geocoding_utils import get_coordinates_for_address

class TestGeocodingUtils(unittest.TestCase):

    @patch('geocoding_utils.RateLimiter') # Patch RateLimiter first
    @patch('geocoding_utils.Nominatim')   # Then Nominatim
    def test_successful_geocoding(self, MockNominatim, MockRateLimiter):
        # Configure Nominatim mock
        mock_geolocator_instance = MockNominatim.return_value
        mock_location = MagicMock()
        mock_location.latitude = 10.0
        mock_location.longitude = 20.0
        mock_geolocator_instance.geocode.return_value = mock_location
        
        # Configure RateLimiter mock to passthrough the geocode call to the mocked geolocator
        # The actual geocode function passed to RateLimiter is geolocator.geocode
        # RateLimiter wraps this. We need our _geocode_with_limiter to effectively call mock_geolocator_instance.geocode
        mock_rate_limited_func = MagicMock(return_value=mock_location)
        MockRateLimiter.return_value = mock_rate_limited_func

        lat, lon = get_coordinates_for_address("Some Address")
        
        self.assertEqual(lat, 10.0)
        self.assertEqual(lon, 20.0)
        MockNominatim.assert_called_once_with(user_agent="caixaaberta_geocoder/1.0")
        MockRateLimiter.assert_called_once()
        # Check that RateLimiter was called with the geocode method of the Nominatim instance
        self.assertEqual(MockRateLimiter.call_args[0][0], mock_geolocator_instance.geocode)
        self.assertEqual(MockRateLimiter.call_args[1]['min_delay_seconds'], 1)
        
        # Check that the rate-limited function (which internally calls geolocator.geocode) was called
        mock_rate_limited_func.assert_called_once_with("Some Address", timeout=10)

    @patch('geocoding_utils.RateLimiter')
    @patch('geocoding_utils.Nominatim')
    def test_address_not_found(self, MockNominatim, MockRateLimiter):
        mock_geolocator_instance = MockNominatim.return_value
        mock_geolocator_instance.geocode.return_value = None # Simulate address not found
        
        mock_rate_limited_func = MagicMock(return_value=None)
        MockRateLimiter.return_value = mock_rate_limited_func

        with patch('sys.stdout') as mock_stdout: # To suppress potential print warnings if any added in future
            lat, lon = get_coordinates_for_address("Unknown Address")
        
        self.assertIsNone(lat)
        self.assertIsNone(lon)
        mock_rate_limited_func.assert_called_once_with("Unknown Address", timeout=10)

    @patch('geocoding_utils.RateLimiter')
    @patch('geocoding_utils.Nominatim')
    @patch('builtins.print') # To capture print warnings
    def test_geocoder_timeout(self, mock_print, MockNominatim, MockRateLimiter):
        mock_geolocator_instance = MockNominatim.return_value
        # Configure the original geocode method (before RateLimiter) to raise error
        mock_geolocator_instance.geocode.side_effect = GeocoderTimedOut("Timeout error")

        # RateLimiter should propagate this exception if not handled by RateLimiter itself
        # Or, if RateLimiter calls it, the mock_rate_limited_func should raise it.
        mock_rate_limited_func = MagicMock(side_effect=GeocoderTimedOut("Timeout error"))
        MockRateLimiter.return_value = mock_rate_limited_func
        
        lat, lon = get_coordinates_for_address("Address Causing Timeout")
        
        self.assertIsNone(lat)
        self.assertIsNone(lon)
        mock_print.assert_any_call("Warning: Geocoding timed out for address 'Address Causing Timeout'.")

    @patch('geocoding_utils.RateLimiter')
    @patch('geocoding_utils.Nominatim')
    @patch('builtins.print')
    def test_geocoder_service_error(self, mock_print, MockNominatim, MockRateLimiter):
        mock_geolocator_instance = MockNominatim.return_value
        mock_geolocator_instance.geocode.side_effect = GeocoderServiceError("Service error")

        mock_rate_limited_func = MagicMock(side_effect=GeocoderServiceError("Service error"))
        MockRateLimiter.return_value = mock_rate_limited_func

        lat, lon = get_coordinates_for_address("Address Causing Service Error")
        
        self.assertIsNone(lat)
        self.assertIsNone(lon)
        mock_print.assert_any_call("Warning: Geocoding service error for address 'Address Causing Service Error': Service error")

    @patch('geocoding_utils.RateLimiter')
    @patch('geocoding_utils.Nominatim')
    @patch('builtins.print')
    def test_geocoder_unavailable(self, mock_print, MockNominatim, MockRateLimiter):
        mock_geolocator_instance = MockNominatim.return_value
        mock_geolocator_instance.geocode.side_effect = GeocoderUnavailable("Service unavailable")
        
        mock_rate_limited_func = MagicMock(side_effect=GeocoderUnavailable("Service unavailable"))
        MockRateLimiter.return_value = mock_rate_limited_func

        lat, lon = get_coordinates_for_address("Address Causing Unavailable")

        self.assertIsNone(lat)
        self.assertIsNone(lon)
        mock_print.assert_any_call("Warning: Geocoding service (Nominatim) unavailable for address 'Address Causing Unavailable'.")
        
    @patch('geocoding_utils.RateLimiter') # Still need to patch them so they don't run
    @patch('geocoding_utils.Nominatim')
    def test_empty_address(self, MockNominatim, MockRateLimiter):
        # No need to configure .geocode as it shouldn't be called
        lat, lon = get_coordinates_for_address("")
        self.assertIsNone(lat)
        self.assertIsNone(lon)
        MockNominatim.return_value.geocode.assert_not_called() # Geocode itself should not be called by RateLimiter's wrapper
        MockRateLimiter.return_value.assert_not_called() # The rate-limited function should not be called

    @patch('geocoding_utils.RateLimiter')
    @patch('geocoding_utils.Nominatim')
    def test_none_address(self, MockNominatim, MockRateLimiter):
        lat, lon = get_coordinates_for_address(None)
        self.assertIsNone(lat)
        self.assertIsNone(lon)
        MockRateLimiter.return_value.assert_not_called()

    @patch('geocoding_utils.RateLimiter')
    @patch('geocoding_utils.Nominatim')
    def test_whitespace_address(self, MockNominatim, MockRateLimiter):
        lat, lon = get_coordinates_for_address("   ")
        self.assertIsNone(lat)
        self.assertIsNone(lon)
        MockRateLimiter.return_value.assert_not_called()
        
    # Test User-Agent and RateLimiter setup (already partially in test_successful_geocoding)
    @patch('geocoding_utils.RateLimiter')
    @patch('geocoding_utils.Nominatim')
    def test_nominatim_and_ratelimiter_initialization(self, MockNominatim, MockRateLimiter):
        # This test effectively re-runs the import-time logic of geocoding_utils
        # by checking the calls during a typical geocoding operation.
        # For more direct testing of module-level setup, you might need to reload the module
        # or inspect the already initialized _geolocator and _geocode_with_limiter.
        # However, by patching them for other tests, we confirm they are used.

        mock_geolocator_instance = MockNominatim.return_value
        mock_location = MagicMock()
        mock_location.latitude = 1.0
        mock_location.longitude = 1.0
        
        mock_rate_limited_func = MagicMock(return_value=mock_location)
        MockRateLimiter.return_value = mock_rate_limited_func # This is _geocode_with_limiter

        get_coordinates_for_address("Test Address")

        MockNominatim.assert_called_once_with(user_agent="caixaaberta_geocoder/1.0")
        # RateLimiter is initialized at module load time.
        # To test this, we'd need to reload geocoding_utils within the test or inspect its state.
        # The patch on RateLimiter for the class/method is for its *usage*.
        # The call to RateLimiter constructor happens when geocoding_utils is first imported.
        # We can check that the _geocode_with_limiter (which is MockRateLimiter.return_value)
        # was called, implying RateLimiter was set up and used.
        mock_rate_limited_func.assert_called_once_with("Test Address", timeout=10)
        
        # To verify RateLimiter was constructed correctly at module import time:
        # This requires geocoding_utils to be imported *after* the patch is set up,
        # or to use importlib.reload. This is a bit more advanced.
        # For now, seeing it used via mock_rate_limited_func is an indirect confirmation.


if __name__ == '__main__':
    unittest.main()
