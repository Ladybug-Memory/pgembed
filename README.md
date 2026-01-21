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

# pgembed: pip-installable, embedded postgres server + pgvector extension for your python app

pgembed is a fork of [pgserver](https://github.com/orm011/pgserver), a project that lets you build Postgres-backed python apps with the same convenience afforded by an embedded database (ie, alternatives such as sqlite).
If you build your app with pgembed, your app remains wholly pip-installable, saving your users from needing to understand how to setup a postgres server (they simply pip install your app, and postgres is brought in through dependencies), and letting you get started developing quickly: just `pip install pgembed` and `pgembed.get_server(...)`, as shown in this notebook: <a target="_blank" href="https://colab.research.google.com/github/anomalyco/pgembed/blob/master/pgembed-example.ipynb"> <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/> </a>

To achieve this, you need two things which `pgembed` provides
  * python binary wheels for multiple-plaforms with postgres binaries
  * convenience python methods that handle db initialization and server process management, that deals with things that would normally prevent you from running your python app seamlessly on environments like docker containers, a machine you have no root access in, machines with other running postgres servers, google colab, etc.  One main goal of the project is robustness around this.

Additionally, this package includes the [pgvector](https://github.com/pgvector/pgvector) postgres extension, useful for storing associated vector data and for vector similarity queries.

## Basic summary:
* _Pip installable binaries_: built and tested on Manylinux, MacOS and Windows.
* _No sudo or admin rights needed_: Does not require `root` privileges or `sudo`.
* but... _can handle root_: in some environments your python app runs as root, eg docker, google colab, `pgembed` handles this case.
* _Simpler initialization_: `pgembed.get_server(MY_DATA_DIR)` method to initialize data and server if needed, so you don't need to understand `initdb`, `pg_ctl`, port conflicts.
* _Convenient cleanup_: server process cleanup is done for you: when the process using pgembed ends, the server is shutdown, including when multiple independent processes call
`pgembed.get_server(MY_DATA_DIR)` on the same dir (wait for last one). You can blow away your PGDATA dir and start again.
* For lower-level control, wrappers to all binaries, such as `initdb`, `pg_ctl`, `psql`, `pg_config`. Includes header files in case you wish to build some other extension and use it against these binaries.

## Sample Usage

```
cd examples
uv sync
for script in *.py; do
  uv run $script
done
```

Postgres binaries in the package can be found in the directory pointed
to by the `pgembed.POSTGRES_BIN_PATH` to be used directly.

This project was originally based on [](https://github.com/michelp/postgresql-wheel), which provides a linux wheel.
But adds the following differences:
1. binary wheels for multiple platforms (ubuntu x86, MacOS apple silicon, MacOS x86, Windows)
2. postgres python management: cross-platfurm startup and cleanup including many edge cases, runs on colab etc.
3. includes `pgvector` extension but currently excludes `postGIS`
