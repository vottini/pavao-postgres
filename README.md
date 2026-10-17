# pavao-postgres

PostgreSQL backend module for [pavao](https://github.com/vottini/pavao), providing async connection pooling, a task store, and a key-value settings store.

## Installation

Use `pavao-pkg` to vendor this extension into your project:

```
pavao-pkg add github.com/vottini/pavao-postgres@<version>
```

This will:
- Copy the extension files into your project
- Record the metadata in `pavao_packages.json`
- Install the `asyncpg` dependency

The following directories will be created inside your project:

```
your_project/
├── foundation/
│   └── postgres/        ← supplier and task store implementation
└── modules/
    └── postgres/        ← (empty for this extension)
```

After cloning a project that already uses this extension, restore the files with:

```
pavao-pkg sync
```

## Database setup

Before configuring the module, you need a PostgreSQL database and user. Two common ways to create them:

**Using psql:**

```sql
CREATE DATABASE mydb;
CREATE USER myuser WITH PASSWORD 'mypassword';
GRANT ALL PRIVILEGES ON DATABASE mydb TO myuser;
```

**Using pgAdmin:**

1. In the object tree, right-click **Databases** → **Create** → **Database**, set the name and owner.
2. To create a user, right-click **Login/Group Roles** → **Create** → **Login/Group Role**, fill in the name and set a password under the **Definition** tab.
3. To grant access, open the database properties → **Security** tab, add the user with the `ALL` privilege.

## Configuration

Add the following section to your pavao `.yml` configuration file:

```yaml
pavao:
  database:
    postgresql:
      host: localhost       # required — database host
      port: 5432            # optional — defaults to 5432
      database: mydb        # required — database name
      user: myuser          # required — database user
      password: mypassword  # optional — defaults to empty string
      pavao_backend: false  # optional — set to true to enable task store and stored settings
```

All keys are nested under `pavao.database.postgresql`.

## Injected parameters

Once configured, the following parameters become available for injection into pavao tasks:

| Parameter | Type | Description |
|---|---|---|
| `pg_pool` | `asyncpg.Pool` | The shared connection pool |
| `pg_connection` | `asyncpg.Connection` | A dedicated connection, released after the task finishes |
| `task_store` | `PostgresTaskStore` | Task execution tracker — only available when `pavao_backend: true` |
| `stored_settings` | `PostgresStoredSettings` | Database-backed key-value config — only available when `pavao_backend: true` |

## Using PostgreSQL as pavao's backend

By default, pavao uses a built-in SQLite database to track task executions and store settings. Setting `pavao_backend: true` activates this module as a **replacement** for that built-in backend: it injects `task_store` and `stored_settings` into the framework, creates the required schema on first run, and takes over all execution tracking and configuration persistence.

**You must also disable pavao's native SQLite backend**, otherwise both backends will be active simultaneously. Add this to your `application.yml`:

```yaml
pavao:
  database:
    sqlite:
      enabled: false
```

The full configuration with PostgreSQL as the backend looks like this:

```yaml
pavao:
  database:
    sqlite:
      enabled: false
    postgresql:
      host: localhost
      port: 5432
      database: mydb
      user: myuser
      password: mypassword
      pavao_backend: true
```

When `pavao_backend: true`, the module automatically creates the required tables inside a `pavao` schema on first run:

- `pavao.settings` — typed key-value configuration
- `pavao.task` — registered task definitions
- `pavao.task_execution` — execution history with timezone-aware timestamps
- `pavao.task_output` — captured stdout/stderr per execution

The database user must have permission to create schemas and tables, or the `pavao` schema and its tables must be created manually beforehand using the SQL files in `foundation/database/`.
