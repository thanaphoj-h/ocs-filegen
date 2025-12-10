import os
from dotenv import load_dotenv

from psycopg import OperationalError, DatabaseError, connect

load_dotenv()


class Database:

    def __init__(self):
        self.primary_conn = None
        self.primary_config = self._get_config('DB_PRIMARY')
        self.secondary_conn = None
        self.secondary_config = self._get_config('DB_SECONDARY')


    def _get_config(self, prefix: str):
        config = {
            'host': os.getenv(f'{prefix}_HOST'),
            'port': os.getenv(f'{prefix}_PORT'),
            'database': os.getenv(f'{prefix}_NAME'),
            'user': os.getenv(f'{prefix}_USER'),
            'password': os.getenv(f'{prefix}_PASS'),
        }
        if not all(config.values()):
            raise RuntimeError(f"Missing required environment variables: {prefix}")
        return config


    def _connect_to_database(self, host: str, port: str, database: str, user: str, password: str):
        conn = None
        try:
            # Establish connection
            conn = connect(host=host, port=port, dbname=database, user=user, password=password)
            # Connection Verify
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            return conn
        except OperationalError as err:
            if conn:
                conn.close()
            raise ValueError(
                f"Operational Error connecting to {database}@{host}:{port}. Please check credentials/network. "
                f"Error: {err}")
        except DatabaseError as err:
            if conn:
                conn.close()
            raise ValueError(f"Database Error during verification: {err}")
        except Exception as err:
            if conn:
                conn.close()
            raise ValueError(f"An unexpected error occurred while connect to database: {err}")

    def connect_primary_database(self):
        self.primary_conn = self._connect_to_database(**self.primary_config)
        return self.primary_conn

    def connect_secondary_database(self):
        self.secondary_conn = self._connect_to_database(**self.secondary_config)
        return self.secondary_conn

    def close_all_connection(self):
        # Close primary connection
        if self.primary_conn:
            self.primary_conn.close()
            self.primary_conn = None
        # Close secondary connection
        if self.secondary_conn:
            self.secondary_conn.close()
            self.secondary_conn = None

if __name__ == '__main__':
    db = Database()
    db.connect_primary_database()
    db.connect_secondary_database()
