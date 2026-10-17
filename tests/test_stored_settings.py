import pytest


async def test_get_missing_key(stored_settings):
    assert await stored_settings.get('__nonexistent__') is None


async def test_int_round_trip(stored_settings):
    await stored_settings.set('ss_int', 42)
    assert await stored_settings.get('ss_int') == 42


async def test_str_round_trip(stored_settings):
    await stored_settings.set('ss_str', 'hello world')
    assert await stored_settings.get('ss_str') == 'hello world'


async def test_float_round_trip(stored_settings):
    await stored_settings.set('ss_float', 3.14)
    assert await stored_settings.get('ss_float') == 3.14


async def test_bool_true_round_trip(stored_settings):
    await stored_settings.set('ss_bool_true', True)
    assert await stored_settings.get('ss_bool_true') is True


async def test_bool_false_round_trip(stored_settings):
    await stored_settings.set('ss_bool_false', False)
    assert await stored_settings.get('ss_bool_false') is False


async def test_list_int_round_trip(stored_settings):
    await stored_settings.set('ss_list_int', [1, 2, 3])
    assert await stored_settings.get('ss_list_int') == [1, 2, 3]


async def test_list_str_round_trip(stored_settings):
    await stored_settings.set('ss_list_str', ['a', 'b', 'c'])
    assert await stored_settings.get('ss_list_str') == ['a', 'b', 'c']


async def test_list_bool_round_trip(stored_settings):
    await stored_settings.set('ss_list_bool', [True, False, True])
    assert await stored_settings.get('ss_list_bool') == [True, False, True]


async def test_empty_list_round_trip(stored_settings):
    await stored_settings.set('ss_empty', [])
    assert await stored_settings.get('ss_empty') == []


async def test_upsert_overwrites(stored_settings):
    await stored_settings.set('ss_upsert', 1)
    await stored_settings.set('ss_upsert', 99)
    assert await stored_settings.get('ss_upsert') == 99


async def test_mixed_type_list_raises(stored_settings):
    with pytest.raises(ValueError, match="same type"):
        await stored_settings.set('ss_bad_list', [1, 'two', 3])


async def test_unsupported_type_raises(stored_settings):
    with pytest.raises(ValueError, match="Unsupported"):
        await stored_settings.set('ss_bad_type', object())


async def test_get_raises_on_corrupted_type_in_db(stored_settings, connection):
    await connection.execute(
        "INSERT INTO pavao.settings (name, type, value) VALUES ($1, $2, $3)"
        " ON CONFLICT (name) DO UPDATE SET type = EXCLUDED.type, value = EXCLUDED.value",
        'ss_corrupted', 'datetime', '2024-01-01',
    )
    with pytest.raises(ValueError, match="Unsupported"):
        await stored_settings.get('ss_corrupted')
