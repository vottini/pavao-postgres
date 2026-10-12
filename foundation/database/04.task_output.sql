CREATE TABLE IF NOT EXISTS pavao.task_output (
	id        SERIAL PRIMARY KEY,
	execution INTEGER REFERENCES pavao.task_execution(id) NOT NULL,
	error     BOOLEAN,
	instant   TIMESTAMP,
	content   TEXT
);
