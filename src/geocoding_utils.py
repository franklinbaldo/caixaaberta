# geocoding_utils.py
import time
from pathlib import Path

import duckdb
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError

# --- Cache de coordenadas ---
# O cache vive em DuckDB, o mesmo motor que o pipeline já usa para unir os
# CSVs. Um arquivo SQLite continua disponível sob demanda, exportado pela
# extensão sqlite do próprio DuckDB — não é preciso manter um segundo
# gravador só para produzir esse formato.
DB_NAME = "cache.duckdb"
TABLE_NAME = "coords"

_connection = None


def _cache_connection():
    """Conexão única com o cache, aberta na primeira consulta."""
    global _connection
    if _connection is None:
        _connection = duckdb.connect(DB_NAME)
        _connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                address VARCHAR PRIMARY KEY,
                lat DOUBLE,
                lon DOUBLE
            )
            """
        )
    return _connection


def close_cache_db():
    """Fecha a conexão com o cache. O arquivo continua no disco."""
    global _connection
    if _connection is not None:
        _connection.close()
        _connection = None


def _get_cached_coords(address: str) -> tuple | None:
    """Devolve (lat, lon) do cache, ou None quando o endereço é novo."""
    result = _cache_connection().execute(
        f"SELECT lat, lon FROM {TABLE_NAME} WHERE address = ?", (address,)
    ).fetchone()
    return result if result else None


def _cache_coords(address: str, lat: float, lon: float):
    """Grava as coordenadas de um endereço, ignorando quem já está lá."""
    _cache_connection().execute(
        f"INSERT OR IGNORE INTO {TABLE_NAME} (address, lat, lon) VALUES (?, ?, ?)",
        (address, lat, lon),
    )


def export_cache_to_sqlite(sqlite_path: Path | str = "cache.sqlite"):
    """Exporta o cache para um arquivo SQLite, pela extensão do DuckDB."""
    path = Path(sqlite_path)
    path.unlink(missing_ok=True)

    conn = _cache_connection()
    conn.execute("INSTALL sqlite")
    conn.execute("LOAD sqlite")
    conn.execute(f"ATTACH '{path}' AS cache_sqlite (TYPE sqlite)")
    try:
        conn.execute(
            f"CREATE TABLE cache_sqlite.{TABLE_NAME} AS "
            f"SELECT address, lat, lon FROM {TABLE_NAME}"
        )
    finally:
        conn.execute("DETACH cache_sqlite")
    return path


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
