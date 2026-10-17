
CREATE TABLE IF NOT EXISTS pavao.task (
	id      SERIAL PRIMARY KEY,
	package TEXT,
	name    TEXT NOT NULL,
	enabled BOOLEAN DEFAULT TRUE,
	UNIQUE (package, name)
);

