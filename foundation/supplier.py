
import os
import asyncpg
import pavao

from .pg_task_store import PostgresTaskStore, PostgresStoredSettings

schema_dir = pavao.path_to('database')
schema_files = sorted(f for f in os.listdir(schema_dir) if f.endswith('.sql'))

database_schema = {
	"host": str,
	"port": 5432,
	"database": str,
	"user": str,
	"password": "",
	"task_store": False,
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

			if self.config['task_store']:
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

		if self.config['task_store']:
			async def provide_task_store():
				return PostgresTaskStore(self.pool)

			async def provide_stored_settings():
				return PostgresStoredSettings(self.pool)

			params['task_store'] = [provide_task_store, None]
			params['stored_settings'] = [provide_stored_settings, None]

		return params

	#------------------------------------------------------------

	async def retrieve_pool(self): return self.pool
	async def create_connection(self): return await self.pool.acquire()
	async def close_connection(self, conn): await self.pool.release(conn)

	#------------------------------------------------------------

	async def shutdown(self):
		if self.pool is not None:
			await self.pool.close()
			pavao.Logger.info("PostgreSQL database pool closed")
			self.pool = None
