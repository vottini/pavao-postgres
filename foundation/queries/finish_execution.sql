UPDATE pavao.task_execution
SET execution_end = $1,
    success = $2
WHERE id = $3;
