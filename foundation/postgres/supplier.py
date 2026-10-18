
import os
import asyncpg
import pavao

pavao.from_module('pg_task_store.py').import_item('PostgresTaskStore').import_item('PostgresStoredSettings')

#-------------------------------------------------------------------------------

class SmartConnection:
	__slots__ = ('_conn',)

	def __init__(self, conn):
		self._conn = conn

	def _resolve(self, query, args, kwargs):
		if not isinstance(query, str):
			return query.sql, query.args(kwargs)
		return query, args

	async def execute(self, query, *args, **kwargs):
		sql, positional = self._resolve(query, args, kwargs)
		return await self._conn.execute(sql, *positional)

	async def fetch(self, query, *args, **kwargs):
		sql, positional = self._resolve(query, args, kwargs)
		return await self._conn.fetch(sql, *positional)

	async def fetchrow(self, query, *args, **kwargs):
		sql, positional = self._resolve(query, args, kwargs)
		return await self._conn.fetchrow(sql, *positional)

	async def fetchval(self, query, *args, **kwargs):
		sql, positional = self._resolve(query, args, kwargs)
		return await self._conn.fetchval(sql, *positional)

	def transaction(self):
		return self._conn.transaction()

#-------------------------------------------------------------------------------

database_schema = {
	"host": str,
	"port": 5432,
	"database": str,
	"user": str,
	"password": "",
	"pavao_backend": False,
}

class PostgresSupplier(pavao.ParameterSupplier):
	def __init__(self):
		self.pool = None
		self.enabled = False
		self.config = None


	def configure(self, loaded_settings):
		db_settings = loaded_settings.get_setting(
			"pavao.database.postgresql",
			database_schema)

		if db_settings is not None:
			self.config = db_settings
			self.enabled = True


	async def initialize(self):
		if not self.enabled:
			return

		try:
			connect_args = {k: self.config[k]
				for k in ('host', 'port', 'database', 'user', 'password')}

			self.pool = await asyncpg.create_pool(**connect_args)
			pavao.Logger.info("PostgreSQL database connected")
			pavao.register_mapping_class(asyncpg.Record)

			if self.config['pavao_backend']:
				schema_dir = pavao.path_to('database')
				schema_files = sorted(f for f in os.listdir(schema_dir) if f.endswith('.sql'))
				async with self.pool.acquire() as conn:
					for filename in schema_files:
						with open(os.path.join(schema_dir, filename)) as fd:
							await conn.execute(fd.read())
				pavao.Logger.info("PostgreSQL task store schema ready")

		except Exception as e:
			pavao.Logger.error("Error while setting up PostgreSQL database: " + str(e))
			raise pavao.PavaoException("Unable to start PostgreSQL database pool")


	def parameters_supplied(self):
		if not self.enabled:
			return dict()

		params = {
			'pg_pool': [
				self.retrieve_pool,
				None
			],

			'pg_connection': [
				self.create_connection,
				self.close_connection
			]
		}

		if self.config['pavao_backend']:
			async def provide_task_store():
				return PostgresTaskStore(self.pool)

			async def provide_stored_settings():
				return PostgresStoredSettings(self.pool)

			params['task_store'] = [provide_task_store, None]
			params['stored_settings'] = [provide_stored_settings, None]

		return params

	#------------------------------------------------------------

	async def retrieve_pool(self): return self.pool
	async def create_connection(self): return SmartConnection(await self.pool.acquire())
	async def close_connection(self, conn): await self.pool.release(conn._conn)

	#------------------------------------------------------------

	async def shutdown(self):
		if self.pool is not None:
			await self.pool.close()
			pavao.Logger.info("PostgreSQL database pool closed")
			self.pool = None
