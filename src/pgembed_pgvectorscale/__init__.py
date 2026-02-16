__version__ = "0.1.8"

EXTENSION_NAME = "pgvectorscale"
EXTENSION_SO = "vectorscale.so"
EXTENSION_CREATE = "vectorscale"

def get_extension_path():
    from pathlib import Path
    pkg_dir = Path(__file__).parent
    so_path = pkg_dir / EXTENSION_SO
    if so_path.exists():
        return so_path

    try:
        import pgembed
        base_lib = pgembed.EXTENSION_LIB_PATH
        bundled = base_lib / EXTENSION_SO
        if bundled.exists():
            return bundled
    except ImportError:
        pass

    return None

def get_pg_lib_path():
    import subprocess
    try:
        result = subprocess.run(
            ['pg_config', '--pkglibdir'],
            capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None
