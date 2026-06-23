#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
from typing import override

import asyncpg, asyncio, json
from pandas import DataFrame, Timestamp

from utils.livetester_component import LiveTesterComponent
from utils.utils import Log, CONFIG
from collections import deque
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
class DBListener(LiveTesterComponent):
    """
    Database Listener given the PostgreSQL credentals, entry point and a "callbacks" dictionary
    ("cbacks") which has a series of table names as keys, and a function attach to each, as values.
    The function would run whenever the table that has the name of its key gets updated within the
    Database for whatever the reason.
    """

    TYPE_MAPPINGS = dict(float = "::double precision",
      int = "::integer", bool = "::boolean", str = "")

    URI_FORMAT = "postgresql://{user}:{pass}@{ip}/{name}"

    QUERY_NOTIFY = """ CREATE OR REPLACE 
    FUNCTION notify_{0}() RETURNS trigger AS $$
    BEGIN PERFORM pg_notify('{0}', json_build_object(
        'operation', TG_OP, 'table', TG_TABLE_NAME,
        'time', CURRENT_TIMESTAMP, 'query_tag',
        current_setting('app.query_tag', TRUE))::text );
    RETURN NEW; END; $$ LANGUAGE plpgsql; """

    QUERY_TRIG = """ DROP TRIGGER IF EXISTS {0}_trigger ON {1};
    CREATE TRIGGER {0}_trigger AFTER INSERT OR UPDATE OR DELETE ON {1}
    FOR EACH STATEMENT EXECUTE FUNCTION notify_{0}(); """

    SEP = "-" * 100

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __init__(self, name: str, uri: str = None):

        self._name = name
        if (uri is not None): self.uri = uri
        else: self.uri = self.URI_FORMAT.format(**CONFIG["DB"])
        self._db = None
        self._write_db = None  # Separate connection for write operations

        # Add queue and condition for task management
        self._query_queue = deque()
        self._query_condition = asyncio.Condition()
        self._db_locker = asyncio.Lock()
        self._write_db_locker = asyncio.Lock()  # Separate lock for write operations

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __getitem__(self, key: str):
        df: DataFrame = self._tables.get(key, None)
        if df is not None: return df.copy()

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def __iter__(self):
        return iter(self._tables)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def connect(self):
        self._db = await asyncpg.connect(self.uri)
        self._write_db = await asyncpg.connect(self.uri)  # Create separate connection for writing
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    def execute_queued(self, query: str, *args, timeout: float = None):
        """
        Execute a query by adding it to the queue.
        
        Args:
            query: The SQL query to execute
            *args: Query parameters
            timeout: Optional timeout
        """
        task = (query, args, timeout)

        # Create a task to add the query to the queue and notify the condition
        async def _add_to_queue():
            async with self._query_condition:
                self._query_queue.append(task)
                self._query_condition.notify()

        # Schedule the task without waiting for it
        asyncio.create_task(_add_to_queue())

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _run_updater(self):
        """
        Process queries from the queue sequentially.
        Waits if queue is empty.
        
        Returns:
            Result of the executed query
        """
        while True:
            try:
                async with self._query_condition:
                    while not self._query_queue:
                        await self._query_condition.wait()

                    query, args, timeout = self._query_queue.popleft()
                    # Use the write connection and lock for executing queries
                    async with self._write_db_locker:
                        #Log.info(f"{self.SEP}\nExecuting query:\n{query}\n{self.SEP}")
                        await self._write_db.execute(query, *args, timeout=timeout)
            except asyncpg.exceptions.UniqueViolationError as EXC:
                Log.error(repr(EXC))
            except Exception as EXC:
                Log.exception(f"Error in _process_queries: {repr(EXC)}", EXC)
                return

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def query(self, query: str, *args, timeout: float = None, record_class: type = None):
        # For read-only queries, use the write connection to avoid conflicts with listener
        async with self._write_db_locker:
            return await self._write_db.fetch(query, *args, timeout=timeout, record_class=record_class)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def query_table(self, query: str, *args, timeout: float = None, record_class: type = None):
        raw: list[dict] = await self.query(query, *args, timeout = timeout, record_class = record_class)
        return DataFrame(raw, columns = raw[0].keys())

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def transaction(self, query: str):
        async with self._write_db_locker:
            async with self._write_db.transaction() as tx:
                return await tx.fetch(query)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def prepare(self, query: str):
        async with self._write_db_locker:
            return await self._write_db.prepare(query)
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def listen(self):
        queue = asyncio.Queue()
        def handler(conn, pid, channel, payload):
            queue.put_nowait((channel, payload))
        await self._db.add_listener(self._name, handler)
        changes: dict = json.loads((await queue.get())[1])
        oper, table, ts, query_tag = changes.values()
        delay = (Timestamp.utcnow() - Timestamp(ts)).total_seconds()
        Log.info(f"DB {oper} @ \"{table}\" ({delay * 1e6:.0f} us)")

        # Skip callback if no callback needed
        if (query_tag == f"skip_{self._name}"): return

        # Use write_db for table queries to prevent blocking the listener connection
        query = f"SELECT * FROM {table};"
        content = await self.query_table(query)
        if table not in self._cbacks: return
        else: callback = self._cbacks[table]
        self._tables[table] = content
        if (callback is not None):
            callback(content)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def _run_listener(self, cbacks: dict):
        self._cbacks = cbacks.copy()
        self._tables = dict.fromkeys(cbacks.keys())
        await self.connect()  # Ensure we have both connections established
        await self.read_all()

        for table in self._tables:
            try:
                query_notify = self.QUERY_NOTIFY.format(self._name)
                query_trig = self.QUERY_TRIG.format(self._name, table)
                # Set up triggers using the write connection
                async with self._write_db_locker:
                    await self._write_db.execute(query_notify)
                    await self._write_db.execute(query_trig)
                
                # Set up listener on the dedicated listener connection
                async with self._db_locker:
                    await self._db.execute(f"LISTEN {self._name}")
            except Exception as EXC:
                Log.error(f"Error in run: {repr(EXC)}")
                Log.exception(EXC)

        self.running = True
        while self.running:
            try: await self.listen()
            except asyncio.TimeoutError:
                try: 
                    async with self._db_locker:
                        await self._db.execute("SELECT 'checking conn'")
                except Exception as EXC:
                    Log.error(f"Error in listen: {repr(EXC)}")
                    Log.exception(EXC)
                    break
            except Exception as EXC:
                Log.error(f"Error in listen: {repr(EXC)}")
                Log.exception(EXC)
                break

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    @override
    async def start(self, cbacks: dict):
        self._task_updater = asyncio.create_task(self._run_updater())
        self._task_listener = asyncio.create_task(self._run_listener(cbacks))

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def read_all(self):
        for table in self._tables:
            query = f"SELECT * FROM {table};"
            self._tables[table] = await self.query_table(query)

    #▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
    async def stop(self):
        async with self._db_locker:
            await self._db.close()
        async with self._write_db_locker:
            await self._write_db.close()
        self._task_updater.cancel()
        self._task_listener.cancel()

#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
#███████████████████████████████████████████████████████████████████████████████████████████████████████████
#▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀
#▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄
if (__name__ == "__main__"):

    pg = DBListener(["zz_test"],
        uri = "postgresql://postgres:123456wxyz*S@212.117.171.68:5432/postgres")

    async def main():
        asyncio.create_task(pg.start())
        while True: await asyncio.sleep(1)

    asyncio.run(main())
