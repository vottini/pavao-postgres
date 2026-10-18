from namedquery import named


async def test_positional_fetchrow(connection):
    row = await connection.fetchrow("SELECT $1::int AS val", 42)
    assert row['val'] == 42


async def test_named_fetchrow(connection):
    _q = named("SELECT :val::int AS result")
    row = await connection.fetchrow(_q, val=99)
    assert row['result'] == 99


async def test_positional_fetch(connection):
    rows = await connection.fetch(
        "SELECT v FROM unnest(ARRAY[$1::int, $2::int]) AS v", 1, 2
    )
    assert [r['v'] for r in rows] == [1, 2]


async def test_named_fetch(connection):
    _q = named("SELECT v FROM unnest(ARRAY[:a::int, :b::int]) AS v")
    rows = await connection.fetch(_q, a=3, b=4)
    assert [r['v'] for r in rows] == [3, 4]


async def test_positional_fetchval(connection):
    val = await connection.fetchval("SELECT $1::text", "hello")
    assert val == "hello"


async def test_named_fetchval(connection):
    _q = named("SELECT :msg::text")
    val = await connection.fetchval(_q, msg="world")
    assert val == "world"


async def test_positional_execute(connection):
    await connection.execute("CREATE TEMP TABLE _sc_pos (id INT)")
    await connection.execute("INSERT INTO _sc_pos VALUES ($1)", 7)
    val = await connection.fetchval("SELECT id FROM _sc_pos")
    assert val == 7


async def test_named_execute(connection):
    _create = named("CREATE TEMP TABLE _sc_named (label TEXT)")
    await connection.execute(_create)
    _insert = named("INSERT INTO _sc_named VALUES (:label)")
    await connection.execute(_insert, label="hello")
    val = await connection.fetchval("SELECT label FROM _sc_named")
    assert val == "hello"


async def test_transaction_commits(connection):
    async with connection.transaction():
        await connection.execute("CREATE TEMP TABLE _sc_tx (val INT)")
        await connection.execute("INSERT INTO _sc_tx VALUES ($1)", 7)
    row = await connection.fetchrow("SELECT val FROM _sc_tx")
    assert row['val'] == 7
