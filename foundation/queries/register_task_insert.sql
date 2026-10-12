INSERT INTO pavao.task (package, name, enabled)
VALUES ($1, $2, TRUE)
ON CONFLICT (package, name) DO NOTHING;
