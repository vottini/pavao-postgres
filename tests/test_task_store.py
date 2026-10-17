from datetime import datetime, timezone


async def test_register_task_returns_id_and_enabled(task_store):
    task_id, enabled = await task_store.register_task('register_new', 'test_pkg')
    assert isinstance(task_id, int)
    assert enabled is True


async def test_register_task_is_idempotent(task_store):
    id1, _ = await task_store.register_task('idempotent', 'test_pkg')
    id2, _ = await task_store.register_task('idempotent', 'test_pkg')
    assert id1 == id2


async def test_start_execution_returns_id(task_store):
    task_id, _ = await task_store.register_task('start_exec', 'test_pkg')
    exec_id = await task_store.start_execution(task_id, datetime.now(timezone.utc))
    assert isinstance(exec_id, int)


async def test_finish_execution(task_store):
    task_id, _ = await task_store.register_task('finish_exec', 'test_pkg')
    exec_id = await task_store.start_execution(task_id, datetime.now(timezone.utc))
    await task_store.finish_execution(exec_id, datetime.now(timezone.utc), True)


async def test_record_output(task_store):
    task_id, _ = await task_store.register_task('record_out', 'test_pkg')
    exec_id = await task_store.start_execution(task_id, datetime.now(timezone.utc))
    await task_store.record_output(exec_id, datetime.now(timezone.utc), 'hello stdout', False)
    await task_store.record_output(exec_id, datetime.now(timezone.utc), 'oops stderr', True)


async def test_list_tasks_includes_registered(task_store):
    await task_store.register_task('list_me', 'test_pkg')
    tasks = await task_store.list_tasks()
    assert any(t['name'] == 'list_me' and t['package'] == 'test_pkg' for t in tasks)


async def test_get_executions_excludes_running(task_store):
    task_id, _ = await task_store.register_task('exec_running', 'test_pkg')
    exec_id = await task_store.start_execution(task_id, datetime.now(timezone.utc))
    execs = await task_store.get_executions(task_id)
    assert not any(e['id'] == exec_id for e in execs)


async def test_get_executions_includes_finished(task_store):
    task_id, _ = await task_store.register_task('exec_finished', 'test_pkg')
    exec_id = await task_store.start_execution(task_id, datetime.now(timezone.utc))
    await task_store.finish_execution(exec_id, datetime.now(timezone.utc), True)
    execs = await task_store.get_executions(task_id)
    assert any(e['id'] == exec_id for e in execs)


async def test_get_executions_by_day(task_store):
    task_id, _ = await task_store.register_task('exec_day', 'test_pkg')
    exec_id = await task_store.start_execution(task_id, datetime.now(timezone.utc))
    await task_store.finish_execution(exec_id, datetime.now(timezone.utc), True)
    today = datetime.now(timezone.utc).date()
    execs = await task_store.get_executions(task_id, day=today)
    assert any(e['id'] == exec_id for e in execs)


async def test_get_outputs_ordered_by_instant(task_store):
    task_id, _ = await task_store.register_task('output_order', 'test_pkg')
    exec_id = await task_store.start_execution(task_id, datetime.now(timezone.utc))
    await task_store.record_output(exec_id, datetime.now(timezone.utc), 'first', False)
    await task_store.record_output(exec_id, datetime.now(timezone.utc), 'second', False)
    outputs = await task_store.get_outputs(exec_id)
    assert len(outputs) == 2
    assert outputs[0]['content'] == 'first'
    assert outputs[1]['content'] == 'second'


async def test_execution_success_flag_false(task_store):
    task_id, _ = await task_store.register_task('success_false', 'test_pkg')
    exec_id = await task_store.start_execution(task_id, datetime.now(timezone.utc))
    await task_store.finish_execution(exec_id, datetime.now(timezone.utc), False)
    execs = await task_store.get_executions(task_id)
    match = next(e for e in execs if e['id'] == exec_id)
    assert match['success'] is False


async def test_output_error_flag(task_store):
    task_id, _ = await task_store.register_task('output_error_flag', 'test_pkg')
    exec_id = await task_store.start_execution(task_id, datetime.now(timezone.utc))
    await task_store.record_output(exec_id, datetime.now(timezone.utc), 'bad', True)
    outputs = await task_store.get_outputs(exec_id)
    assert outputs[0]['error'] is True
