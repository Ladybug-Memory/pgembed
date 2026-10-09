__version__ = "0.1.8"

EXTENSION_NAME = "pgsql_http"
EXTENSION_SO_FILES = ("http.so", "http.dylib")
EXTENSION_CREATE = "http"


def get_extension_path():
    from pathlib import Path

    pkg_dir = Path(__file__).parent
    for filename in EXTENSION_SO_FILES:
        so_path = pkg_dir / filename
        if so_path.exists():
            return so_path

    try:
        import pgembed

        base_lib = pgembed.EXTENSION_LIB_PATH
        for filename in EXTENSION_SO_FILES:
            bundled = base_lib / "postgresql" / filename
            if bundled.exists():
                return bundled
    except ImportError:
        pass

    return None


def get_extension_share_path():
    from pathlib import Path

    try:
        import pgembed

        base_share = (
            Path(__file__).parent / "pginstall" / "share" / "postgresql" / "extension"
        )
        control_file = base_share / f"{EXTENSION_CREATE}.control"
        if control_file.exists():
            return base_share

        base_share = (
            pgembed.EXTENSION_LIB_PATH.parent / "share" / "postgresql" / "extension"
        )
        control_file = base_share / f"{EXTENSION_CREATE}.control"
        if control_file.exists():
            return base_share
    except ImportError:
        pass
    return None
