import psycopg2
from db_config import postgres_config
from logger import logger

class Database:
    def __init__(self, config=postgres_config):
        self.config = config
        self.conn = self._connect()

    def _connect(self):
        """Creates and returns a connection to the PostgreSQL database."""
        try:
            conn = psycopg2.connect(**self.config)
            logger.info(f"Connected to database {self.config['database']} at {self.config['host']}:{self.config['port']}")
            return conn
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            return None

    def _handle_fetch(self, cursor):
        """Handles queries that return data (e.g., SELECT). Returns True on success."""
        # Thực hiện fetchall để đảm bảo query hoàn tất, nhưng chỉ trả về True theo yêu cầu
        cursor.fetchall()
        return True

    def _handle_transaction(self, cursor):
        """Handles queries that modify data (e.g., INSERT, UPDATE, DELETE). Returns True on success."""
        self.conn.commit()
        return True
    
    def execute_query(self, query, params=None):
        """
        Executes a SQL query. 
        Returns True if successful, False if an error occurs.
        """
        # Check connection status and reconnect if needed
        if self.conn is None or self.conn.closed:
            logger.warning("Connection lost. Attempting to reconnect...")
            self.conn = self._connect()
            if self.conn is None:
                return False

        cursor = None
        try:
            cursor = self.conn.cursor()
            # logger.info(f"Query: {query}")
            # logger.info(f"Params: {params}")
            cursor.execute(query, params)

            # Check if the query returns rows (e.g., SELECT)
            if cursor.description:
                return self._handle_fetch(cursor)
            else:
                # For INSERT, UPDATE, DELETE, etc.
                return self._handle_transaction(cursor)

        except Exception as e:
            # Rollback the transaction on error to reset the connection state
            if self.conn:
                self.conn.rollback()
            logger.error(f"Error executing query: {query}. Error: {e}")
            return False
        finally:
            if cursor:
                cursor.close()

    def close(self):
        """Closes the database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()

# Export a default instance for convenience
db = Database()
