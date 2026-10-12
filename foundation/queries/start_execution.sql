INSERT INTO pavao.task_execution (task, execution_start)
VALUES ($1, $2)
RETURNING id;
