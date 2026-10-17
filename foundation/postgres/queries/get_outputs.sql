SELECT instant, error, content
FROM pavao.task_output
WHERE execution = $1
ORDER BY instant;
