SELECT id, success, execution_start, execution_end
FROM pavao.task_execution
WHERE task = $1
  AND execution_end IS NOT NULL;
