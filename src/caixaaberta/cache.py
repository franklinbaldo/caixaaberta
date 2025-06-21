import sqlite3
from pathlib import Path
import logging # Import logging
import sys # For direct run logging config

# Get a logger instance for this module
logger = logging.getLogger(__name__)

# --- SQLite Cache Setup ---
DB_PATH = Path("data/cache.sqlite")
TABLE_NAME = "coords"

def init_cache_db():
    """Initializes the SQLite database and coords table if they don't exist.
    Ensures the data directory exists."""
    try:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
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
        logger.debug(f"Cache database {DB_PATH} initialized/verified.")
    except Exception as e:
        logger.error(f"Failed to initialize cache database {DB_PATH}: {e}", exc_info=True)

init_cache_db()

def get_cached_coords(address: str) -> tuple | None:
    """Retrieves coordinates from cache. Returns (lat, lon) or None if not found."""
    if not DB_PATH.exists():
        logger.warning(f"Cache DB {DB_PATH} not found, attempting to re-initialize.")
        init_cache_db()
        if not DB_PATH.exists():
             logger.error(f"Cache DB {DB_PATH} could not be created. Cannot get cached coords.")
             return None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"SELECT lat, lon FROM {TABLE_NAME} WHERE address = ?", (address,))
        result = cursor.fetchone()
        conn.close()
        if result:
            logger.debug(f"Cache hit for address '{address}'")
        else:
            logger.debug(f"Cache miss for address '{address}'")
        return result
    except Exception as e:
        logger.error(f"Error getting cached coords for '{address}': {e}", exc_info=True)
        return None

def cache_coords(address: str, lat: float, lon: float):
    """Stores coordinates in cache."""
    if not DB_PATH.exists():
        logger.warning(f"Cache DB {DB_PATH} not found, attempting to re-initialize.")
        init_cache_db()
        if not DB_PATH.exists():
            logger.error(f"Cache DB {DB_PATH} could not be created. Cannot cache coords for '{address}'.")
            return
    conn = None  # Initialize conn to None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(f"INSERT INTO {TABLE_NAME} (address, lat, lon) VALUES (?, ?, ?)", (address, lat, lon))
        conn.commit()
        logger.debug(f"Cached coordinates for address '{address}'")
    except sqlite3.IntegrityError:
        logger.debug(f"Address '{address}' already in cache. No update performed.")
        pass
    except Exception as e:
        logger.error(f"Error caching coords for '{address}': {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

def clear_cache():
    """Deletes the cache file."""
    try:
        if DB_PATH.exists():
            DB_PATH.unlink()
            logger.info(f"Cache '{DB_PATH}' deleted.")
        init_cache_db()
    except Exception as e:
        logger.error(f"Error clearing cache '{DB_PATH}': {e}", exc_info=True)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout,
                        format="%(asctime)s | %(levelname)s | %(module)s:%(lineno)d | %(message)s")
    logger.info(f"Initializing cache at {DB_PATH.resolve()}")
    clear_cache()
    cache_coords("Test Address 1, City, State", 10.0, 20.0)
    logger.info(get_cached_coords("Test Address 1, City, State"))
    cache_coords("Test Address 2, City, State", 30.0, 40.0)
    logger.info(get_cached_coords("Test Address 2, City, State"))
