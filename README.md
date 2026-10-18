# pavao-postgres

PostgreSQL backend module for [pavao](https://github.com/vottini/pavao), providing async
connection pooling, a task store, and a key-value settings store.

## Usage

Once installed and configured, pavao injects a `pg_connection` into any endpoint
or task that declares it as a parameter:

```python
from namedquery import named

user_bp = """

root {
  id: Integer,
  name: String,
  email: String
}

"""

_get_user = named('SELECT id, name, email FROM users WHERE id = :id')


@pavao.generates_json(string_blueprint=user_bp)
async def get_user(response, path_variables, pg_connection):
    row = await pg_connection.fetchrow(_get_user, id=path_variables['user_id'])

    if row is None:
        response.status(404)
        return

    return row


def pavao_endpoint():
    return {
        'method': 'GET',
        'path': '/users/{user_id:int}',
        'processor': get_user,
    }

```

`named(sql)` translates `:param` placeholders to asyncpg positional parameters **once at module load time**, so there is no translation overhead per request. The plain positional form (`$1, $2, ...`) also works — `pg_connection` accepts both transparently.

The connection is acquired from the pool before the handler runs and released automatically when it returns. `@pavao.generates_json` serializes the returned dict as the response body. See [pavao decorators](https://pavao.readthedocs.io/en/latest/sections/decorators.html) for more information.

`pg_connection` accepts both named queries (via [namedquery](https://github.com/vottini/namedquery)) and raw positional queries interchangeably:

```python
# named parameters — translated once at module load, zero cost per call
_by_id   = named("SELECT * FROM users WHERE id = :id")
_by_role = named("SELECT * FROM users WHERE role = :role AND active = :active")

await pg_connection.fetchrow(_by_id, id=1)
await pg_connection.fetch(_by_role, role='admin', active=True)

# raw positional — standard asyncpg syntax, works as-is
await pg_connection.fetchrow("SELECT * FROM users WHERE id = $1", 1)
await pg_connection.fetch("SELECT * FROM users WHERE role = $1 AND active = $2", 'admin', True)
```

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

After cloning a project that uses this extension, download the postgres backend with:

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
