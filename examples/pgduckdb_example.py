import tempfile
import pgembed
import sqlalchemy as sa
from sqlalchemy_utils import database_exists, create_database
import os
from pathlib import Path
import subprocess
import time
import glob

# Override the ensure_pgdata_inited method to configure pg_duckdb before server starts
original_ensure_pgdata_inited = pgembed.PostgresServer.ensure_pgdata_inited


def patched_ensure_pgdata_inited(self):
    # Call original initialization
    original_ensure_pgdata_inited(self)

    # Now modify postgresql.conf to include pg_duckdb
    conf_file = self.pgdata / "postgresql.conf"
    venv_path = Path(__file__).parent / ".venv"

    # Dynamically search for pgduckdb library (handles different Python versions and platforms)
    # Try common patterns for different platforms:
    # - Linux/macOS: .venv/lib/python*/site-packages/...
    # - Windows: .venv/Lib/site-packages/...
    search_patterns = [
        venv_path
        / "lib/python*/site-packages/pgembed/pginstall/lib/postgresql/pg_duckdb.*",  # Linux/macOS
        venv_path
        / "Lib/site-packages/pgembed/pginstall/lib/postgresql/pg_duckdb.*",  # Windows
    ]

    matching_libs = []
    for pattern in search_patterns:
        matching_libs = glob.glob(str(pattern))
        if matching_libs:
            break

    if not matching_libs:
        searched = "\n  ".join(str(p) for p in search_patterns)
        raise FileNotFoundError(
            f"Could not find pg_duckdb library. Searched:\n  {searched}"
        )

    # Use the parent directory of the first match
    pg_duckdb_lib = Path(matching_libs[0]).parent
    print(f"Found pg_duckdb library at: {pg_duckdb_lib}")

    # Read existing config
    with open(conf_file, "r") as f:
        config = f.read()

    # Only add if not already present
    if "pg_duckdb" not in config:
        with open(conf_file, "a") as f:
            f.write(f"\n# pg_duckdb configuration\n")
            f.write(f"shared_preload_libraries = 'pg_duckdb'\n")
            f.write(f"dynamic_library_path = '{pg_duckdb_lib}'\n")
        print(f"✓ Configured pg_duckdb in postgresql.conf")


# Apply the patch
pgembed.PostgresServer.ensure_pgdata_inited = patched_ensure_pgdata_inited

with tempfile.TemporaryDirectory() as tmpdir:
    print("Starting PostgreSQL server with pg_duckdb extension...")
    with pgembed.get_server(tmpdir) as pg:
        database_name = "testdb"
        uri = pg.get_uri(database_name)

        if not database_exists(uri):
            create_database(uri)

        engine = sa.create_engine(uri, isolation_level="AUTOCOMMIT")
        conn = engine.connect()

        # Try to explicitly create the pg_duckdb extension
        try:
            print("Creating pg_duckdb extension...")
            conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS pg_duckdb"))
            print("✓ pg_duckdb extension created successfully!")
            duckdb_available = True
        except Exception as e:
            print(f"✗ pg_duckdb extension not available: {e}")
            print(
                "  Note: pg_duckdb requires shared_preload_libraries configuration at server startup"
            )
            duckdb_available = False

        print("\nCreating events table in PostgreSQL...")

        conn.execute(
            sa.text(
                """
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                event_type VARCHAR(100) NOT NULL,
                user_id INTEGER NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                data JSONB
            )
        """
            )
        )

        # Insert sample data with mix of recent and historical data
        conn.execute(
            sa.text(
                """
            INSERT INTO events (event_type, user_id, timestamp, data) VALUES
                -- Historical data (older than 1 day)
                ('login', 1, NOW() - INTERVAL '3 days', '{"ip": "192.168.1.1", "device": "mobile"}'),
                ('purchase', 1, NOW() - INTERVAL '3 days', '{"product_id": 123, "amount": 99.99}'),
                ('login', 2, NOW() - INTERVAL '2 days', '{"ip": "192.168.1.2", "device": "desktop"}'),
                ('view_product', 2, NOW() - INTERVAL '2 days', '{"product_id": 456}'),
                ('purchase', 2, NOW() - INTERVAL '2 days', '{"product_id": 456, "amount": 149.99}'),
                ('logout', 1, NOW() - INTERVAL '2 days', '{}'),
                ('login', 3, NOW() - INTERVAL '2 days', '{"ip": "192.168.1.3", "device": "tablet"}'),
                ('view_product', 3, NOW() - INTERVAL '2 days', '{"product_id": 789}'),
                ('purchase', 3, NOW() - INTERVAL '2 days', '{"product_id": 789, "amount": 79.99}'),
                ('logout', 3, NOW() - INTERVAL '2 days', '{}'),
                -- Recent data (within 1 day)
                ('login', 4, NOW() - INTERVAL '5 hours', '{"ip": "192.168.1.4", "device": "mobile"}'),
                ('view_product', 4, NOW() - INTERVAL '4 hours', '{"product_id": 100}'),
                ('purchase', 4, NOW() - INTERVAL '3 hours', '{"product_id": 100, "amount": 199.99}'),
                ('login', 5, NOW() - INTERVAL '2 hours', '{"ip": "192.168.1.5", "device": "desktop"}'),
                ('logout', 4, NOW() - INTERVAL '1 hour', '{}')
        """
            )
        )

        print("✓ Inserted sample event data")

        # Check how much historical vs recent data we have
        result_counts = conn.execute(
            sa.text(
                """
            SELECT
                COUNT(*) FILTER (WHERE timestamp < NOW() - INTERVAL '1 day') as historical_count,
                COUNT(*) FILTER (WHERE timestamp >= NOW() - INTERVAL '1 day') as recent_count
            FROM events
        """
            )
        )
        counts = result_counts.fetchone()
        print(
            f"  Historical records (>1 day old): {counts.historical_count}, Recent records: {counts.recent_count}"
        )

        # Create DuckDB native columnar table for historical data
        if duckdb_available:
            print("\nCreating DuckDB native columnar table for historical data...")
            try:
                # Create a TEMP DuckDB table (stored in DuckDB's columnar format)
                # Note: pg_duckdb requires TEMP tables unless MotherDuck is enabled
                conn.execute(
                    sa.text(
                        """
                    CREATE TEMP TABLE events_historical (
                        id INTEGER,
                        event_type VARCHAR(100),
                        user_id INTEGER,
                        timestamp TIMESTAMP,
                        data TEXT
                    ) USING duckdb
                """
                    )
                )
                print("✓ Created DuckDB columnar table 'events_historical' (TEMP)")

                # Copy historical data (older than 1 day) to the columnar table
                print("Copying historical data to columnar table...")
                result_copy = conn.execute(
                    sa.text(
                        """
                    INSERT INTO events_historical
                    SELECT id, event_type, user_id, timestamp, data::TEXT
                    FROM events
                    WHERE timestamp < NOW() - INTERVAL '1 day'
                """
                    )
                )
                print(
                    f"✓ Copied {result_copy.rowcount} historical records to columnar table"
                )

            except Exception as e:
                print(f"✗ Could not create DuckDB columnar table: {e}")
                duckdb_available = False

        # Enable DuckDB execution if available
        if duckdb_available:
            try:
                conn.execute(sa.text("SET duckdb.force_execution = true"))
                print("✓ Enabled DuckDB columnar execution mode")
            except Exception as e:
                print(f"✗ Could not enable DuckDB execution: {e}")

        print("\nRunning analytics queries on PostgreSQL table...")

        result = conn.execute(
            sa.text(
                """
            SELECT
                event_type,
                COUNT(*) as event_count,
                COUNT(DISTINCT user_id) as unique_users
            FROM events
            GROUP BY event_type
            ORDER BY event_count DESC
        """
            )
        )

        print("\nEvent analytics summary (all data):")
        for row in result:
            print(
                f"  {row.event_type}: {row.event_count} events, {row.unique_users} unique users"
            )

        # Query the DuckDB columnar table for historical analytics
        if duckdb_available:
            print("\nRunning analytics queries on DuckDB columnar table...")

            result_columnar = conn.execute(
                sa.text(
                    """
                SELECT
                    event_type,
                    COUNT(*) as event_count,
                    COUNT(DISTINCT user_id) as unique_users
                FROM events_historical
                GROUP BY event_type
                ORDER BY event_count DESC
            """
                )
            )

            print("\nHistorical event analytics (from columnar table):")
            for row in result_columnar:
                print(
                    f"  {row.event_type}: {row.event_count} events, {row.unique_users} unique users"
                )

            # Time-based aggregation on columnar table
            result_time = conn.execute(
                sa.text(
                    """
                SELECT
                    DATE_TRUNC('day', timestamp) as day,
                    event_type,
                    COUNT(*) as events_per_day
                FROM events_historical
                GROUP BY DATE_TRUNC('day', timestamp), event_type
                ORDER BY day, event_type
            """
                )
            )

            print("\nDaily event breakdown (historical data from columnar table):")
            for row in result_time:
                print(f"  {row.day}: {row.event_type} - {row.events_per_day} events")

        # Time-based aggregation query on regular table
        result2 = conn.execute(
            sa.text(
                """
            SELECT
                DATE_TRUNC('hour', timestamp) as hour,
                event_type,
                COUNT(*) as events_per_hour
            FROM events
            WHERE timestamp >= NOW() - INTERVAL '1 day'
            GROUP BY DATE_TRUNC('hour', timestamp), event_type
            ORDER BY hour, event_type
        """
            )
        )

        print("\nRecent hourly event breakdown (from PostgreSQL table):")
        for row in result2:
            print(f"  {row.hour}: {row.event_type} - {row.events_per_hour} events")

        if duckdb_available:
            # Verify DuckDB is actually being used
            result3 = conn.execute(sa.text("SHOW duckdb.force_execution"))
            duckdb_status = result3.scalar()
            print(f"\n✓ DuckDB execution status: {duckdb_status}")
            print(
                "✓ Example completed using pg_duckdb with native columnar storage!"
            )
            print("  - Historical data stored in DuckDB columnar table")
            print("  - Recent data in PostgreSQL row-based table")
            print(
                "  - Analytics queries executed on columnar table for optimal performance"
            )
        else:
            print("\n✗ Example completed with PostgreSQL (pg_duckdb was not available)")
            print(
                "  To enable pg_duckdb, the extension must be loaded via shared_preload_libraries"
            )

        conn.close()
