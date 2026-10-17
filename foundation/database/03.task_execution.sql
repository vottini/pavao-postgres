
CREATE TABLE IF NOT EXISTS pavao.task_execution (
	id               SERIAL PRIMARY KEY,
	task             INTEGER REFERENCES pavao.task(id) NOT NULL,
	execution_start  TIMESTAMP WITH TIME ZONE,
	execution_end    TIMESTAMP WITH TIME ZONE,
	success          BOOLEAN
);

