__version__ = "0.1.8"

EXTENSION_NAME = "pgvector"
EXTENSION_SO = "vector.so"
EXTENSION_CREATE = "vector"

def get_extension_path():
    from pathlib import Path
    pkg_dir = Path(__file__).parent
    so_path = pkg_dir / EXTENSION_SO
    if so_path.exists():
        return so_path

    try:
        import pgembed
        base_lib = pgembed.EXTENSION_LIB_PATH
        bundled = base_lib / "postgresql" / EXTENSION_SO
        if bundled.exists():
            return bundled
    except ImportError:
        pass

    return None

def get_extension_share_path():
    from pathlib import Path
    try:
        import pgembed
        base_share = Path(__file__).parent / "pginstall" / "share" / "postgresql" / "extension"
        control_file = base_share / f"{EXTENSION_NAME}.control"
        if control_file.exists():
            return base_share

        base_share = pgembed.EXTENSION_LIB_PATH.parent / "share" / "postgresql" / "extension"
        control_file = base_share / f"{EXTENSION_NAME}.control"
        if control_file.exists():
            return base_share
    except ImportError:
        pass
    return None
