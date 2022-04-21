import pytest

from firepit.query import Aggregation
from firepit.query import Column
from firepit.query import Count
from firepit.query import CountUnique
from firepit.query import Filter
from firepit.query import Group
from firepit.query import InvalidComparisonOperator
from firepit.query import InvalidQuery
from firepit.query import Join
from firepit.query import Limit
from firepit.query import Offset
from firepit.query import Order
from firepit.query import Predicate
from firepit.query import Projection
from firepit.query import Query
from firepit.query import Table
from firepit.query import Unique


@pytest.mark.parametrize(
    'lhs, op, rhs, expected_len', [
        ('foo', '=', 99, 1),
        ('bar', '>=', 99, 1),
        ('baz', 'LIKE', '%blah%', 1),
    ]
)
def test_predicate(lhs, op, rhs, expected_len):
    p1 = Predicate(lhs, op, rhs)
    text = p1.render('?')
    assert str(rhs) not in text
    assert '?' in text
    assert len(p1.values) == expected_len
    assert p1.values[0] == rhs


@pytest.mark.parametrize(
    'lhs, op, rhs, expected_text', [
        ('foo', '=', 'null', '("foo" IS NULL)'),
        ('bar', '!=', 'NULL', '("bar" IS NOT NULL)'),
        ('baz[*]', '=', 'NULL', '("baz" IS NULL)'),
        ('next.name[*]', '!=', 'null', '("next.name" IS NOT NULL)'),
    ]
)
def test_predicate_nulls(lhs, op, rhs, expected_text):
    p1 = Predicate(lhs, op, rhs)
    text = p1.render('?')
    assert text == expected_text
    assert len(p1.values) == 0


@pytest.mark.parametrize(
    'lhs, op, rhs, expected_text, values', [
        (Predicate('foo', '>', 5), 'AND', Predicate('bar', 'LIKE', 'baz%'),
         '(("foo" > ?) AND ("bar" LIKE ?))', (5, "baz%")),
        (Predicate('foo', '=', 5), 'OR',
         Predicate('bar', 'IN',
                   Query([Table('my_table'), Filter([Predicate('blah', '=', 0)]), Projection(['blah'])])),
         '(("foo" = ?) OR ("bar" IN (SELECT "blah" FROM "my_table" WHERE ("blah" = ?))))', (5, 0)),
    ]
)
def test_complex_predicate(lhs, op, rhs, expected_text, values):
    p1 = Predicate(lhs, op, rhs)
    text = p1.render('?')
    assert text == expected_text
    assert p1.values == values


@pytest.mark.parametrize(
    'lhs, op, rhs', [
        ('foo', 'asdf', 99),
        ('bar', 6, 99),
        ('baz', 'UNLIKE', '%blah%'),
        ('baz', '<', None),
    ]
)
def test_bad_comp_op(lhs, op, rhs):
    with pytest.raises(InvalidComparisonOperator):
        _ = Predicate(lhs, op, rhs)


def test_query_1():
    query = Query()
    query.append(Table('my_table'))
    qtext, values = query.render('%s')
    assert qtext == 'SELECT * FROM "my_table"'

    query.append(Projection(['foo', 'bar', 'baz']))
    qtext, values = query.render('%s')
    assert qtext == 'SELECT "foo", "bar", "baz" FROM "my_table"'

    p1 = Predicate('foo', '!=', 0)
    p2 = Predicate('bar', 'LIKE', r'%blah%')
    where = Filter([p1, p2])
    query.append(where)
    qtext, values = query.render('%s')
    assert qtext == r'SELECT "foo", "bar", "baz" FROM "my_table" WHERE ("foo" != %s) AND ("bar" LIKE %s)'
    assert len(values) == 2
    assert values[0] == 0
    assert values[1] == r'%blah%'

    order = Order([('bar', Order.ASC), ('baz', Order.DESC)])
    query.append(order)
    qtext, values = query.render('%s')
    assert qtext == 'SELECT "foo", "bar", "baz" FROM "my_table" WHERE ("foo" != %s) AND ("bar" LIKE %s) ORDER BY "bar" ASC, "baz" DESC'
    assert len(values) == 2
    assert values[0] == 0
    assert values[1] == r'%blah%'


def test_query_2():
    query = Query()
    query.append(Table('my_table'))
    qtext, values = query.render('%s')
    assert qtext == 'SELECT * FROM "my_table"'

    p1 = Predicate('foo', '!=', 0)
    p2 = Predicate('bar', 'LIKE', r'%blah%')
    filt = Filter([p1, p2], Filter.OR)
    query.append(filt)
    qtext, values = query.render('%s')
    assert qtext == r'SELECT * FROM "my_table" WHERE (("foo" != %s) OR ("bar" LIKE %s))'
    assert len(values) == 2
    assert values[0] == 0
    assert values[1] == r'%blah%'

    query.append(Limit(10))
    qtext, values = query.render('%s')
    assert qtext == r'SELECT * FROM "my_table" WHERE (("foo" != %s) OR ("bar" LIKE %s)) LIMIT 10'
    assert len(values) == 2
    assert values[0] == 0
    assert values[1] == r'%blah%'

    query.append(Offset(10))
    qtext, values = query.render('?')
    assert qtext == r'SELECT * FROM "my_table" WHERE (("foo" != ?) OR ("bar" LIKE ?)) LIMIT 10 OFFSET 10'
    assert len(values) == 2
    assert values[0] == 0
    assert values[1] == r'%blah%'


def test_query_3():
    query = Query()
    query.append(Table('my_table'))
    p1 = Predicate('foo', '!=', 'null')
    p2 = Predicate('bar', 'LIKE', r'%blah%')
    where = Filter([p1, p2])
    query.append(where)
    qtext, values = query.render('%s')
    assert qtext == r'SELECT * FROM "my_table" WHERE ("foo" IS NOT NULL) AND ("bar" LIKE %s)'
    assert len(values) == 1
    assert values[0] == r'%blah%'

    query.append(Projection(['foo', 'bar', 'baz']))
    qtext, values = query.render('%s')
    assert qtext == 'SELECT "foo", "bar", "baz" FROM "my_table" WHERE ("foo" IS NOT NULL) AND ("bar" LIKE %s)'
    assert len(values) == 1
    assert values[0] == r'%blah%'


def test_filter_list():
    query = Query()
    query.append(Table('my_table'))
    p1 = Predicate('foo[*]', '=', 'bar')
    where = Filter([p1])
    query.append(where)
    qtext, values = query.render('%s')
    assert qtext == r'SELECT * FROM "my_table" WHERE ("foo" LIKE %s)'
    assert len(values) == 1
    assert values[0] == r'%bar%'


def test_filter_list_not_like():
    query = Query()
    query.append(Table('my_table'))
    p1 = Predicate('foo[*]', '!=', 'bar')
    where = Filter([p1])
    query.append(where)
    qtext, values = query.render('%s')
    assert qtext == r'SELECT * FROM "my_table" WHERE ("foo" NOT LIKE %s)'
    assert len(values) == 1
    assert values[0] == r'%bar%'


def test_filter_in():
    query = Query()
    query.append(Table('my_table'))
    p1 = Predicate('foo', 'IN', [1, 2, 3])
    where = Filter([p1])
    query.append(where)
    qtext, values = query.render('%s')
    assert qtext == r'SELECT * FROM "my_table" WHERE ("foo" IN (%s, %s, %s))'
    assert len(values) == 3
    assert values[0] == 1
    assert values[1] == 2
    assert values[2] == 3


def test_double_filter():
    query = Query()
    query.append(Table('my_table'))
    query.append(Filter([Predicate('foo', '=', 0),
                         Predicate('bar', '=', 1)], op=Filter.OR))
    query.append(Filter([Predicate('baz', '!=', 2),
                         Predicate('buz', '!=', 3)]))
    qtext, values = query.render('%s')
    assert qtext == r'SELECT * FROM "my_table" WHERE (("foo" = %s) OR ("bar" = %s)) AND ("baz" != %s) AND ("buz" != %s)'
    assert len(values) == 4
    assert values[0] == 0
    assert values[1] == 1
    assert values[2] == 2
    assert values[3] == 3


def test_filter_in():
    query = Query()
    query.append(Table('my_table'))
    p1 = Predicate('foo', 'IN', [1, 2, 3])
    where = Filter([p1])
    query.append(where)
    qtext, values = query.render('%s')
    assert qtext == r'SELECT * FROM "my_table" WHERE ("foo" IN (%s, %s, %s))'
    assert len(values) == 3
    assert values[0] == 1
    assert values[1] == 2
    assert values[2] == 3


def test_group():
    query = Query()
    query.append(Table('my_table'))
    p1 = Predicate('foo', '!=', 'null')
    p2 = Predicate('bar', 'LIKE', r'%blah%')
    query.append(Filter([p1, p2]))
    query.append(Group(['baz']))
    query.append(Aggregation([('SUM', 'foo', 'TotalFoo')]))
    qtext, values = query.render('%s')
    assert qtext == r'SELECT "baz", SUM("foo") AS "TotalFoo" FROM "my_table" WHERE ("foo" IS NOT NULL) AND ("bar" LIKE %s) GROUP BY "baz"'
    assert len(values) == 1
    assert values[0] == r'%blah%'
    query.append(Filter([Predicate('TotalFoo', '>=', 10)]))
    qtext, values = query.render('%s')
    assert qtext == r'SELECT "baz", SUM("foo") AS "TotalFoo" FROM "my_table" WHERE ("foo" IS NOT NULL) AND ("bar" LIKE %s) GROUP BY "baz" HAVING ("TotalFoo" >= %s)'
    assert len(values) == 2
    assert values[0] == r'%blah%'
    assert values[1] == 10


def test_group_with_proj():
    query = Query()
    query.append(Table('my_table'))
    query.append(Projection(['foo', 'bar', 'baz']))
    query.append(Group(['baz']))
    with pytest.raises(InvalidQuery) as e:
        query.append(Aggregation([('SUM', 'foo', 'TotalFoo')]))


def test_agg_without_group():
    query = Query()
    query.append(Table('my_table'))
    query.append(Aggregation([('SUM', 'foo', 'TotalFoo')]))
    qtext, values = query.render('%s')
    assert qtext == 'SELECT SUM("foo") AS "TotalFoo" FROM "my_table"'


def test_agg_without_alias():
    query = Query()
    query.append(Table('my_table'))
    query.append(Aggregation([('SUM', 'foo')]))
    qtext, values = query.render('%s')
    assert qtext == 'SELECT SUM("foo") AS "sum" FROM "my_table"'


def test_count_1():
    query = Query()
    query.append(Table('my_table'))
    query.append(Count())
    qtext, values = query.render('%s')
    assert qtext == 'SELECT COUNT(*) AS "count" FROM "my_table"'


def test_count_2():
    query = Query()
    query.append(Table('my_table'))
    query.append(Filter([Predicate('foo', '=', 1)]))
    query.append(Count())
    qtext, values = query.render('?')
    assert qtext == 'SELECT COUNT(*) AS "count" FROM "my_table" WHERE ("foo" = ?)'
    assert len(values) == 1
    assert values[0] == 1


def test_count_group():
    query = Query()
    query.append(Table('my_table'))
    query.append(Group(['baz']))
    query.append(Aggregation([('COUNT', None, 'count')]))
    qtext, values = query.render('?')
    # Since we group, we need the group value and count
    assert qtext == 'SELECT "baz", COUNT(*) AS "count" FROM "my_table" GROUP BY "baz"'


def test_unique():
    query = Query()
    query.append(Table('my_table'))
    query.append(Unique())
    qtext, values = query.render('%s')
    assert qtext == 'SELECT DISTINCT * FROM "my_table"'


def test_unique_count():
    query = Query()
    query.append(Table('my_table'))
    query.append(Unique())
    query.append(Count())
    qtext, values = query.render('%s')
    assert qtext == 'SELECT COUNT(*) AS "count" FROM (SELECT DISTINCT * FROM "my_table") AS tmp'


def test_proj_unique():
    query = Query()
    query.append(Table('my_table'))
    query.append(Projection(['foo', 'bar']))
    query.append(Unique())
    qtext, values = query.render('%s')
    assert qtext == 'SELECT DISTINCT "foo", "bar" FROM "my_table"'


def test_countunique():
    query = Query()
    query.append(Table('my_table'))
    query.append(CountUnique())
    qtext, values = query.render('%s')
    assert qtext == 'SELECT COUNT(*) AS "count" FROM (SELECT DISTINCT * FROM "my_table") AS tmp'


def test_proj_countunique():
    query = Query()
    query.append(Table('my_table'))
    query.append(Projection(['foo', 'bar']))
    query.append(CountUnique())
    qtext, values = query.render('%s')
    assert qtext == 'SELECT COUNT(DISTINCT "foo", "bar") AS "count" FROM "my_table"'


def test_proj_1_unique():
    query = Query()
    query.append(Table('my_table'))
    query.append(Projection(['foo']))
    query.append(Unique())
    qtext, values = query.render('%s')
    assert qtext == 'SELECT DISTINCT "foo" FROM "my_table"'


def test_proj_unique_count():
    query = Query()
    query.append(Table('my_table'))
    query.append(Projection(['foo', 'bar']))
    query.append(Unique())
    query.append(Count())
    qtext, values = query.render('%s')
    assert qtext == 'SELECT COUNT(DISTINCT "foo", "bar") AS "count" FROM "my_table"'


def test_join():
    query = Query()
    query.append(Table('left_table'))
    query.append(Join('right_table', 'left_col', '=', 'right_col'))
    qtext, values = query.render('%s')
    assert qtext == 'SELECT * FROM "left_table" INNER JOIN "right_table" ON "left_table"."left_col" = "right_table"."right_col"'


def test_join_3():
    query = Query()
    query.append(Table('table1'))
    query.append(Join('table2', 'col1', '=', 'col2'))
    query.append(Join('table3', 'col3', '=', 'col4'))
    qtext, values = query.render('%s')
    assert qtext == 'SELECT * FROM "table1" INNER JOIN "table2" ON "table1"."col1" = "table2"."col2" INNER JOIN "table3" ON "table2"."col3" = "table3"."col4"'


def test_join_filter():
    query = Query()
    query.append(Table('left_table'))
    query.append(Join('right_table', 'left_col', '=', 'right_col'))
    query.append(Filter([Predicate('foo', '=', 'bar')]))
    query.append(Projection(['baz']))
    query.append(Unique())
    qtext, values = query.render('%s')
    assert qtext == 'SELECT DISTINCT "baz" FROM "left_table" INNER JOIN "right_table" ON "left_table"."left_col" = "right_table"."right_col" WHERE ("foo" = %s)'
    assert len(values) == 1
    assert values[0] == 'bar'


def test_join_without_table():
    query = Query()
    with pytest.raises(InvalidQuery):
        query.append(Join('right_table', 'left_col', '=', 'right_col'))


def test_implicit_table():
    query = Query('my_table')
    qtext, values = query.render('%s')
    assert qtext == 'SELECT * FROM "my_table"'

    query.append(Projection(['foo', 'bar', 'baz']))
    query.append(Order(['foo']))
    qtext, values = query.render('%s')
    assert qtext == 'SELECT "foo", "bar", "baz" FROM "my_table" ORDER BY "foo" ASC'


def test_explicit_table_in_order():
    query = Query('my_table')
    query.append(Order([Column('foo', 'my_table')]))
    qtext, values = query.render('%s')
    assert qtext == 'SELECT * FROM "my_table" ORDER BY "my_table"."foo" ASC'


def test_explicit_table_in_order_desc():
    query = Query('my_table')
    query.append(Order([(Column('foo', 'my_table'), Order.DESC)]))
    qtext, values = query.render('%s')
    assert qtext == 'SELECT * FROM "my_table" ORDER BY "my_table"."foo" DESC'


def test_query_init_list():
    query = Query([Table('my_table'), Projection(['foo', 'bar', 'baz'])])
    qtext, values = query.render('%s')
    assert qtext == 'SELECT "foo", "bar", "baz" FROM "my_table"'


def test_init_list_join_filter():
    # Same as test_join_filter but using init list
    query = Query([
        Table('left_table'),
        Join('right_table', 'left_col', '=', 'right_col'),
        Filter([Predicate('foo', '=', 'bar')]),
        Projection(['baz']),
        Unique(),
    ])
    qtext, values = query.render('%s')
    assert qtext == 'SELECT DISTINCT "baz" FROM "left_table" INNER JOIN "right_table" ON "left_table"."left_col" = "right_table"."right_col" WHERE ("foo" = %s)'
    assert len(values) == 1
    assert values[0] == 'bar'


def test_subquery():
    subquery = Query()
    subquery.append(Table('my_table'))
    p1 = Predicate('foo', '>', 0)
    where = Filter([p1])
    subquery.append(where)

    query = Query()
    query.append(subquery)
    query.append(Group(['baz']))
    query.append(Aggregation([('SUM', 'foo', 'TotalFoo')]))
    qtext, values = query.render('%s')
    assert qtext == r'SELECT "baz", SUM("foo") AS "TotalFoo" FROM (SELECT * FROM "my_table" WHERE ("foo" > %s)) AS s1 GROUP BY "baz"'
    assert len(values) == 1
    assert values[0] == 0


def test_subquery_in_predicate():
    subquery = Query('some_view')
    subquery.append(Projection(['some_ref']))

    query = Query('some-type')
    query.append(Filter([Predicate('id', 'IN', subquery)]))
    qtext, values = query.render('%s')
    assert qtext == r'SELECT * FROM "some-type" WHERE ("id" IN (SELECT "some_ref" FROM "some_view"))'
    assert len(values) == 0


def test_filter_with_set_table():
    query = Query()
    query.table = Table('my_table')
    p1 = Predicate('foo', '=', 0)
    p2 = Predicate('bar', '=', 1)
    filt = Filter([p1, p2], Filter.OR)
    filt.set_table('my_table')  # Not really needed in this example
    query.where.append(filt)
    qtext, values = query.render('%s')
    assert qtext == r'SELECT * FROM "my_table" WHERE (("my_table"."foo" = %s) OR ("my_table"."bar" = %s))'
    assert len(values) == 2
    assert values[0] == 0
    assert values[1] == 1
