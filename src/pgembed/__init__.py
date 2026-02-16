from ._commands import *
from .postgres_server import PostgresServer, get_server
from pathlib import Path
import logging

_logger = logging.getLogger('pgembed')

EXTENSION_LIB_PATH = Path(__file__).parent / "pginstall" / "lib"

AVAILABLE_EXTENSIONS = {}

EXTENSION_PACKAGES = {
    'pgvector': 'pgembed_pgvector',
    'pgvectorscale': 'pgembed_pgvectorscale',
    'pgtextsearch': 'pgembed_pgtextsearch',
}

EXTENSION_SO_FILES = {
    'pgvector': 'vector.so',
    'pgvectorscale': 'vectorscale.so',
    'pgtextsearch': 'pg_textsearch.so',
    'pg_duckdb': 'pg_duckdb.so',
}

def _detect_extensions():
    global AVAILABLE_EXTENSIONS

    for name, pkg_name in EXTENSION_PACKAGES.items():
        try:
            ext_pkg = __import__(pkg_name)
            ext_path = ext_pkg.get_extension_path()
            if ext_path and ext_path.exists():
                AVAILABLE_EXTENSIONS[name] = True
                _logger.info(f"Detected extension from package {pkg_name}: {name}")
                continue
        except ImportError:
            pass

        so_file = EXTENSION_SO_FILES.get(name)
        if so_file and (EXTENSION_LIB_PATH / so_file).exists():
            AVAILABLE_EXTENSIONS[name] = True
            _logger.info(f"Detected extension from bundled lib: {name}")
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

def get_extension_create_name(name: str) -> str:
    """Get the SQL extension creation name for an extension.

    Args:
        name: Extension name (e.g., 'pgvector', 'pgtextsearch')

    Returns:
        The SQL name to use when creating the extension.
    """
    create_names = {
        'pgvector': 'vector',
        'pgvectorscale': 'vectorscale',
        'pgtextsearch': 'pg_textsearch',
        'pg_duckdb': 'pg_duckdb',
    }
    return create_names.get(name, name)

_detect_extensions()
