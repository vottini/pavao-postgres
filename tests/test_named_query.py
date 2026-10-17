import pytest
from foundation.supplier import named


def test_single_param():
    q = named("SELECT * FROM t WHERE id = :id")
    assert q.sql == "SELECT * FROM t WHERE id = $1"
    assert q.params == ['id']


def test_multiple_params_ordered():
    q = named("INSERT INTO t (a, b) VALUES (:a, :b)")
    assert q.sql == "INSERT INTO t (a, b) VALUES ($1, $2)"
    assert q.params == ['a', 'b']


def test_repeated_param_gets_separate_positions():
    q = named("WHERE x = :val OR y = :val")
    assert q.sql == "WHERE x = $1 OR y = $2"
    assert q.params == ['val', 'val']


def test_cast_operator_preserved():
    q = named("SELECT value::int FROM t WHERE id = :id")
    assert q.sql == "SELECT value::int FROM t WHERE id = $1"


def test_no_params():
    q = named("SELECT 1")
    assert q.sql == "SELECT 1"
    assert q.params == []


def test_args_returns_values_in_param_order():
    q = named("WHERE a = :x AND b = :y")
    assert q.args({'x': 10, 'y': 20}) == [10, 20]


def test_args_raises_on_missing_key():
    q = named("WHERE id = :id")
    with pytest.raises(KeyError):
        q.args({})
