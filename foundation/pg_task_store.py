
import re
import builtins
import collections.abc
from datetime import date, datetime
from typing import Optional, Any

import pavao

#-------------------------------------------------------------------------------

_register_task_insert  = pavao.include('queries/register_task_insert.sql')
_register_task_select  = pavao.include('queries/register_task_select.sql')
_start_execution       = pavao.include('queries/start_execution.sql')
_record_output         = pavao.include('queries/record_output.sql')
_finish_execution      = pavao.include('queries/finish_execution.sql')
_list_tasks            = pavao.include('queries/list_tasks.sql')
_get_executions        = pavao.include('queries/get_executions.sql')
_get_executions_by_day = pavao.include('queries/get_executions_by_day.sql')
_get_outputs           = pavao.include('queries/get_outputs.sql')
_get_setting           = pavao.include('queries/get_setting.sql')
_upsert_setting        = pavao.include('queries/upsert_setting.sql')

#-------------------------------------------------------------------------------

class PostgresTaskStore(pavao.TaskStore):

	def __init__(self, pool):
		self.pool = pool

	async def register_task(self,
		name: str,
		package: Optional[str]) -> tuple:

		async with self.pool.acquire() as conn:
			async with conn.transaction():
				await conn.execute(_register_task_insert, package, name)
				row = await conn.fetchrow(_register_task_select, package, name)

		return row['id'], row['enabled']

	async def start_execution(self,
		task_id: int,
		started_at: datetime) -> int:

		async with self.pool.acquire() as conn:
			row = await conn.fetchrow(_start_execution, task_id, started_at)

		return row['id']

	async def record_output(self,
		execution_id: int,
		instant: datetime,
		content: str,
		is_error: bool) -> None:

		async with self.pool.acquire() as conn:
			await conn.execute(_record_output, execution_id, instant, is_error, content)

	async def finish_execution(self,
		execution_id: int,
		ended_at: datetime,
		success: bool) -> None:

		async with self.pool.acquire() as conn:
			await conn.execute(_finish_execution, ended_at, success, execution_id)

	async def list_tasks(self) -> list:
		async with self.pool.acquire() as conn:
			rows = await conn.fetch(_list_tasks)

		return [dict(row) for row in rows]

	async def get_executions(self,
		task_id: int,
		day: Optional[date] = None) -> list:

		async with self.pool.acquire() as conn:
			if day is None:
				rows = await conn.fetch(_get_executions, task_id)
			else:
				rows = await conn.fetch(_get_executions_by_day, task_id, day)

		return [dict(row) for row in rows]

	async def get_outputs(self,
		execution_id: int) -> list:

		async with self.pool.acquire() as conn:
			rows = await conn.fetch(_get_outputs, execution_id)

		return [dict(row) for row in rows]

#-------------------------------------------------------------------------------

_list_pattern = re.compile(r"\[([^\]]+)\]")
_supported_types = {'int', 'str', 'bool', 'float'}

class PostgresStoredSettings(pavao.StoredSettings):

	def __init__(self, pool):
		self.pool = pool

	async def get(self, key: str) -> Optional[Any]:
		async with self.pool.acquire() as conn:
			row = await conn.fetchrow(_get_setting, key)

		if row is None:
			return None

		typ, value = row['type'], row['value']
		if typ not in _supported_types:
			raise ValueError(f"Unsupported setting type: {typ!r}")

		ctor = getattr(builtins, typ)
		list_search = _list_pattern.search(value)

		if value == '[]':
			return []

		if list_search is not None:
			return [ctor(v) for v in list_search.group(1).split("::")]

		return ctor(value)

	async def set(self, key: str, value: Any) -> None:
		value_type = type(value).__name__

		if isinstance(value, collections.abc.Sequence) and not isinstance(value, str):
			value_type = type(value[0]).__name__ if value else 'str'
			value = f"[{'::'.join(map(str, value))}]"

		async with self.pool.acquire() as conn:
			await conn.execute(_upsert_setting, key, value_type, str(value))
