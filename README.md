![Python Version](https://img.shields.io/badge/python-3.9%2C%203.10%2C%203.11%2C%203.12-blue)
![Postgres Version](https://img.shields.io/badge/PostgreSQL-16.2-blue)

![Linux Support](https://img.shields.io/badge/Linux%20Support-manylinux-green)
![macOS Apple Silicon Support >=11](https://img.shields.io/badge/macOS%20Apple%20Silicon%20Support-%E2%89%A511(BigSur)-green)
![macOS Intel Support => 10.0](https://img.shields.io/badge/macOS%20Intel%20Support-%E2%89%A510.9-green)
![Windows Support >= 2022](https://img.shields.io/badge/Windows%20AMD64%20Support-%E2%89%A52022-green)

[![License](https://img.shields.io/badge/License-Apache%202.0-darkblue.svg)](https://opensource.org/licenses/Apache-2.0)
[![PyPI Package](https://img.shields.io/pypi/v/pgembed?color=darkorange)](https://pypi.org/project/pgembed)
![PyPI - Downloads](https://img.shields.io/pypi/dm/pgembed)


<p align="center">
  <img src="https://raw.githubusercontent.com/Ladybug-Memory/pgembed/main/pgembed_square_small.png"/>
</p>

# pgembed: Embedded PostgreSQL for Agents

pgembed makes it easy to add a full-featured PostgreSQL database to your Python application—no server setup required. Your users simply run `pip install yourapp`, and PostgreSQL comes bundled automatically.

Think of it like SQLite, but with the power of PostgreSQL. Just `pip install pgembed`, call `pgembed.get_server(...)`, and you're ready to go.

<a target="_blank" href="https://colab.research.google.com/github/anomalyco/pgembed/blob/master/pgembed-example.ipynb"> <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/> </a>

## What pgembed gives you

- **Pip-installable PostgreSQL binaries**: Pre-built wheels for Linux, macOS (Apple Silicon & Intel), and Windows
- **No admin rights needed**: Runs without `sudo` or root access
- **Handles edge cases**: Works in Docker containers, Google Colab, and environments with multiple PostgreSQL installations
- **Simple initialization**: `pgembed.get_server(MY_DATA_DIR)` handles `initdb`, port management, and process cleanup automatically
- **Vector search ready**: Includes [pgvector](https://github.com/pgvector/pgvector) and [pgvectorscale](https://github.com/timescale/pgvectorscale) extensions for vector similarity queries and high-performance vector storage
- **Text search ready**: Includes [pg_textsearch](https://github.com/timescale/pg_textsearch) extension for BM25-based full-text search with ranking

## Quick start

```python
import pgembed

# Initialize and start the server
pgembed.get_server("/path/to/my/data/dir")

# Connect and use like any PostgreSQL database
# ... your database code here
# Look in examples/*.py for more complete examples that could be run via uv
```

PostgreSQL binaries are available at `pgembed.POSTGRES_BIN_PATH` if you need direct access to tools like `initdb`, `pg_ctl`, `psql`, or `pg_config`.

## Extensions

pgembed supports optional PostgreSQL extensions. Install with the extensions you need:

```bash
# Base installation (PostgreSQL only)
pip install pgembed

# With specific extensions
pip install pgembed[pgvector]
pip install pgembed[pgvectorscale]
pip install pgembed[pgtextsearch]

# Multiple extensions
pip install pgembed[pgvector,pgvectorscale,pgtextsearch]

# All extensions
pip install pgembed[all]
```

Available extensions:
- `pgvector`: Vector similarity search
- `pgvectorscale`: High-performance vector storage (requires Rust)
- `pgtextsearch`: BM25-based full-text search

### Checking available extensions

```python
import pgembed

# Check which extensions are available
print(pgembed.list_extensions())
# {'pgvector': True, 'pgvectorscale': True, 'pgtextsearch': False, 'pg_duckdb': True}

# Check if a specific extension is available
if pgembed.has_extension('pgvector'):
    # Create the extension
    server.create_extension('vector')
```

### Building specific extensions

To build only specific extensions:

```bash
# Build only pgvector
make pgvector

# Build only pgvectorscale
make pgvectorscale

# Build only pgtextsearch
make pgtextsearch

# Build specific combination
make EXTENSIONS="pgvector pgtextsearch" all
```

## History

pgembed is a fork of [pgserver](https://github.com/orm011/pgserver), which was inspired by [postgresql-wheel](https://github.com/michelp/postgresql-wheel). While those projects focused primarily on Linux wheels, pgembed extends the approach with:

- Multi-platform support (Linux, macOS, Windows)
- Robust process management and cleanup
- Built-in pgvector, pgvectorscale, and pg_textsearch extensions
