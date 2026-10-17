INSERT INTO pavao.settings (name, type, value)
VALUES ($1, $2, $3)
ON CONFLICT (name) DO UPDATE
  SET type  = EXCLUDED.type,
      value = EXCLUDED.value;
