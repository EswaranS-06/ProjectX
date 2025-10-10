"""Resilient Cassandra connector wrapper.

If the DataStax driver isn't fully usable (missing C extensions or Python 3.12 asyncore removal),
this module exposes a SafeCassandraConnector fallback so the pipeline can continue without DB.
"""

import warnings

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra import DependencyException
    _HAS_CASSANDRA = True
except Exception as e:
    # Could be ImportError or DependencyException raised during import-time checks
    _HAS_CASSANDRA = False
    _CASSANDRA_IMPORT_ERROR = e


class _BaseConnector:
    def create_keyspace(self):
        raise NotImplementedError()

    def create_table(self):
        raise NotImplementedError()

    def insert_anomaly(self, row):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()


if _HAS_CASSANDRA:

    class CassandraConnector(_BaseConnector):
        def __init__(self, contact_points=['127.0.0.1'], port=9042, username=None, password=None, keyspace='projectx'):
            """Create a real Cassandra connection using DataStax driver.

            Note: importing the driver can still succeed but runtime operations may fail if
            optional C extensions are missing. This class lets exceptions bubble so callers
            can decide how to handle them.
            """
            if username and password:
                auth_provider = PlainTextAuthProvider(username=username, password=password)
                self.cluster = Cluster(contact_points=contact_points, port=port, auth_provider=auth_provider)
            else:
                self.cluster = Cluster(contact_points=contact_points, port=port)
            self.session = self.cluster.connect()
            self.keyspace = keyspace
            self.create_keyspace()
            self.session.set_keyspace(self.keyspace)

        def create_keyspace(self):
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 1}};
            """)

        def create_table(self):
            self.session.execute(f"""
                CREATE TABLE IF NOT EXISTS anomalies (
                    id UUID PRIMARY KEY,
                    timestamp TIMESTAMP,
                    source TEXT,
                    event_count INT,
                    avg_message_length FLOAT,
                    entropy FLOAT,
                    isolation_forest_label INT,
                    one_class_svm_label INT,
                    dbscan_label INT,
                    ensemble_anomaly INT
                );
            """)

        def insert_anomaly(self, row):
            query = f"""
                INSERT INTO anomalies (
                    id, timestamp, source, event_count, avg_message_length, entropy,
                    isolation_forest_label, one_class_svm_label, dbscan_label, ensemble_anomaly
                )
                VALUES (uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            self.session.execute(query, row)

        def close(self):
            try:
                self.cluster.shutdown()
            except Exception:
                pass

else:

    class CassandraConnector(_BaseConnector):
        """Fallback connector used when cassandra-driver isn't usable.

        Methods are no-ops and will warn the user. This prevents the whole pipeline
        from failing when Cassandra is unavailable or incompatible with the Python runtime.
        """
        def __init__(self, *args, **kwargs):
            warnings.warn(
                "cassandra-driver not available or failed to initialize. Cassandra operations will be no-ops. "
                f"(original error: {_CASSANDRA_IMPORT_ERROR})",
                RuntimeWarning,
            )

        def create_keyspace(self):
            warnings.warn("create_keyspace skipped because cassandra-driver is not available", RuntimeWarning)

        def create_table(self):
            warnings.warn("create_table skipped because cassandra-driver is not available", RuntimeWarning)

        def insert_anomaly(self, row):
            warnings.warn("insert_anomaly skipped because cassandra-driver is not available", RuntimeWarning)

        def close(self):
            pass


__all__ = ['CassandraConnector']
