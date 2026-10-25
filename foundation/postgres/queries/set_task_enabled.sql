
UPDATE pavao.task
	SET enabled = $1
	WHERE id = $2;
