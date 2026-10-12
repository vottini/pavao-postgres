SELECT id, enabled
FROM pavao.task
WHERE package = $1
  AND name = $2;
