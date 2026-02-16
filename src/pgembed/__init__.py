from ._commands import *
from .postgres_server import PostgresServer, get_server
from pathlib import Path
import logging

_logger = logging.getLogger('pgembed')

EXTENSION_LIB_PATH = Path(__file__).parent / "pginstall" / "lib"

AVAILABLE_EXTENSIONS = {}

def _detect_extensions():
    global AVAILABLE_EXTENSIONS
    extensions = {
        'pgvector': 'vector.so',
        'pgvectorscale': 'vectorscale.so',
        'pgtextsearch': 'pg_textsearch.so',
        'pg_duckdb': 'pg_duckdb.so',
    }
    for name, filename in extensions.items():
        if (EXTENSION_LIB_PATH / filename).exists():
            AVAILABLE_EXTENSIONS[name] = True
            _logger.info(f"Detected extension: {name}")
        else:
            AVAILABLE_EXTENSIONS[name] = False

def has_extension(name: str) -> bool:
    """Check if a specific extension is available.

    Args:
        name: Extension name (e.g., 'pgvector', 'pgvectorscale', 'pgtextsearch', 'pg_duckdb')

    Returns:
        True if the extension is available, False otherwise.
    """
    return AVAILABLE_EXTENSIONS.get(name, False)

def list_extensions() -> dict:
    """Return a dictionary of available extensions.

    Returns:
        Dict mapping extension names to availability (True/False)
    """
    return AVAILABLE_EXTENSIONS.copy()

_detect_extensions()
