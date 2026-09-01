import pytest
from unittest.mock import patch, MagicMock
import sqlite3
import sys
from pathlib import Path

# Add src to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from geocoding_utils import (
    get_coordinates_for_address,
    _get_cached_coords,
    _cache_coords,
    _cache_connection,
    close_cache_db,
    export_cache_to_sqlite,
    TABLE_NAME,
)

@pytest.fixture(autouse=True)
def clean_cache_db(tmp_path, monkeypatch):
    """Isola o cache de cada teste num arquivo próprio."""
    import geocoding_utils

    geocoding_utils._geolocators_cache.clear()
    close_cache_db()
    monkeypatch.setattr(geocoding_utils, "DB_NAME", str(tmp_path / "cache.duckdb"))
    _cache_connection()
    yield
    close_cache_db()

@patch("geocoding_utils.Nominatim")
def test_get_coordinates_success(mock_nominatim):
    # Setup mock geocoder
    mock_location = MagicMock()
    mock_location.latitude = 12.34
    mock_location.longitude = 56.78
    mock_geocoder_instance = MagicMock()
    mock_geocoder_instance.geocode.return_value = mock_location
    mock_nominatim.return_value = mock_geocoder_instance

    # Test geocoding new address
    address = "Test Address, City"
    lat, lon = get_coordinates_for_address(address, api_key="test_agent")

    assert lat == 12.34
    assert lon == 56.78
    mock_geocoder_instance.geocode.assert_called_once_with(address, timeout=10)

    # Test caching - should read from DB and NOT call geocode again
    mock_geocoder_instance.geocode.reset_mock()
    lat_cached, lon_cached = get_coordinates_for_address(address, api_key="test_agent")

    assert lat_cached == 12.34
    assert lon_cached == 56.78
    mock_geocoder_instance.geocode.assert_not_called()

@patch("geocoding_utils.Nominatim")
def test_get_coordinates_failure(mock_nominatim):
    # Setup mock geocoder to return None
    mock_geocoder_instance = MagicMock()
    mock_geocoder_instance.geocode.return_value = None
    mock_nominatim.return_value = mock_geocoder_instance

    address = "Invalid Address"
    lat, lon = get_coordinates_for_address(address, api_key="test_agent")

    assert lat is None
    assert lon is None
    mock_geocoder_instance.geocode.assert_called_once_with(address, timeout=10)

    # Check that failed lookups are NOT cached in DB
    cached = _get_cached_coords(address)
    assert cached is None

def test_get_coordinates_empty_address():
    lat, lon = get_coordinates_for_address("")
    assert lat is None
    assert lon is None

    lat, lon = get_coordinates_for_address(None)
    assert lat is None
    assert lon is None

def test_sqlite_cache_functions():
    address = "Direct Cache Test"
    # Insert via internal function
    _cache_coords(address, 99.9, -99.9)

    # Read via internal function
    cached = _get_cached_coords(address)
    assert cached is not None
    assert cached[0] == 99.9
    assert cached[1] == -99.9

    # Test duplicate insert doesn't raise error
    _cache_coords(address, 11.1, -11.1)
    cached2 = _get_cached_coords(address)
    assert cached2[0] == 99.9  # Should keep original values based on IGNORE/pass in IntegrityError


def test_export_cache_to_sqlite_roundtrip(tmp_path):
    _cache_coords("Rua A, Porto Velho, RO", -8.76, -63.90)
    _cache_coords("Rua B, Porto Velho, RO", -8.77, -63.91)

    destino = export_cache_to_sqlite(tmp_path / "cache.sqlite")

    assert destino.exists()
    with sqlite3.connect(destino) as conn:
        rows = conn.execute(
            f"SELECT address, lat, lon FROM {TABLE_NAME} ORDER BY address"
        ).fetchall()

    assert rows == [
        ("Rua A, Porto Velho, RO", -8.76, -63.90),
        ("Rua B, Porto Velho, RO", -8.77, -63.91),
    ]


def test_cache_coords_ignores_a_repeated_address():
    _cache_coords("Rua C", 1.0, 2.0)
    _cache_coords("Rua C", 9.0, 9.0)

    assert _get_cached_coords("Rua C") == (1.0, 2.0)
