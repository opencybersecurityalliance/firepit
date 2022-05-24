"""Utilities for generating SQL while avoiding SQL injection vulns"""
import re

from firepit.validate import validate_name
from firepit.validate import validate_path

COMP_OPS = ['=', '<>', '!=', '<', '>', '<=', '>=', 'LIKE', 'IN', 'IS', 'IS NOT']
PRED_OPS = ['AND', 'OR']
JOIN_TYPES = ['INNER', 'OUTER', 'LEFT OUTER', 'CROSS']
AGG_FUNCS = ['COUNT', 'SUM', 'MIN', 'MAX', 'AVG', 'NUNIQUE']
COL_PATTERN = r"^(\*|[A-Za-z_]+)$"


def _validate_column_name(name):
    if not bool(re.match(COL_PATTERN, name)):
        validate_path(name)  # This is for STIX object paths, not column names...


def _validate_column(col):
    if isinstance(col, str):
        _validate_column_name(col)
    elif isinstance(col, Column):
        _validate_column_name(col.name)
        if col.table:
            validate_name(col.table)
        if col.alias:
            validate_path(col.alias)


class InvalidComparisonOperator(Exception):
    pass


class InvalidPredicateOperator(Exception):
    pass


class InvalidPredicateOperand(Exception):
    pass


class InvalidJoinOperator(Exception):
    pass


class InvalidAggregateFunction(Exception):
    pass


class InvalidQuery(Exception):
    pass


def _quote(obj):
    """Double-quote an SQL identifier if necessary"""
    if isinstance(obj, str):
        if obj == '*':
            return obj
        return f'"{obj}"'
    return str(obj)


class Column:
    """SQL Column name"""

    def __init__(self, name, table=None, alias=None):
        _validate_column_name(name)
        if table:
            validate_name(table)
        if alias:
            validate_path(alias)
        self.name = name
        self.table = table
        self.alias = alias

    def __str__(self):
        if self.table:
            result = f'"{self.table}".{_quote(self.name)}'
        else:
            result = f'{_quote(self.name)}'
        if self.alias:
            result = f'{result} AS "{self.alias}"'
        return result

    def endswith(self, s):
        return str(self).endswith(s)


class CoalescedColumn:
    """First non-null column from a list - used after a JOIN"""

    def __init__(self, names, alias):
        for name in names:
            _validate_column_name(name)
        validate_path(alias)
        self.names = names
        self.alias = alias

    def __str__(self):
        result = ', '.join(self.names)
        result = f'COALESCE({result}) AS "{self.alias}"'
        return result


class Predicate:
    """Row value predicate"""

    def __init__(self, lhs, op, rhs):
        if isinstance(lhs, Predicate):
            if op not in PRED_OPS:
                raise InvalidPredicateOperator(op)
            if not isinstance(rhs, Predicate):
                raise InvalidPredicateOperand(str(rhs))
            self.values = lhs.values + rhs.values
        else:
            table = alias = None
            if op not in COMP_OPS:
                raise InvalidComparisonOperator(op)
            if rhs is None:
                rhs = 'NULL'
            if isinstance(lhs, Column):
                table = lhs.table
                alias = lhs.alias
                lhs = lhs.name
            if '[*]' in lhs:  # STIX list property
                lhs, _, _ = lhs.partition('[*]')  # Need to remove this
                if rhs not in ['null', 'NULL']:
                    rhs = f"%{rhs}%"  # wrap with SQL wildcards since list is encoded as string
                    if op == '=':
                        op = 'LIKE'
                    elif op == '!=':
                        op = 'NOT LIKE'
            if isinstance(lhs, str):
                lhs = Column(lhs, table, alias)
            if rhs in ['null', 'NULL']:
                self.values = ()
                if op not in ['=', '!=', '<>', 'IS', 'IS NOT']:
                    raise InvalidComparisonOperator(op)  # Maybe need different exception here?
            elif isinstance(rhs, (list, tuple)):
                self.values = tuple(rhs)
            elif isinstance(rhs, Column):
                self.values = tuple()
            elif isinstance(rhs, Query):
                _, self.values = rhs.render('IGNORED')
            else:
                self.values = (rhs, )
        self.lhs = lhs
        self.op = op
        self.rhs = rhs

    def render(self, placeholder):
        if isinstance(self.lhs, Predicate):
            text = self.lhs.render(placeholder)
            text += f' {self.op} '
            text += self.rhs.render(placeholder)
            return f'({text})'  # Do we really need parens?

        neg, _, op = self.op.rpartition(' ')

        # Special case for base64-encoded artifacts
        if self.lhs.name == 'payload_bin' and op in ('LIKE', 'MATCHES'):
            if op == 'MATCHES':
                text = f'{neg} match_bin(CAST({placeholder} AS TEXT), {_quote(self.lhs)})'
            elif op == 'LIKE':
                text = f'{neg} like_bin(CAST({placeholder} AS TEXT), {_quote(self.lhs)})'
        elif self.rhs in ['null', 'NULL']:
            if self.op in ['!=', '<>']:
                text = f'({_quote(self.lhs)} IS NOT NULL)'
            elif self.op == '=':
                text = f'({_quote(self.lhs)} IS NULL)'
            else:
                raise InvalidComparisonOperator(self.op)
        elif isinstance(self.rhs, Column):
            text = f'({_quote(self.lhs)} {self.op} {_quote(self.rhs)})'
        elif op == 'IN':
            if isinstance(self.rhs, Query):  # there's probably a better way to detect this
                rhs, _ = self.rhs.render(placeholder)
            else:
                rhs = ', '.join([placeholder] * len(self.rhs))
            text = f'({_quote(self.lhs)} {self.op} ({rhs}))'
        else:
            text = f'({_quote(self.lhs)} {self.op} {placeholder})'
        return text

    def set_table(self, table):
        """Specify table for ALL columns in Predicate"""
        if isinstance(self.lhs, Predicate):
            self.lhs.set_table(table)
        elif isinstance(self.lhs, Column):
            self.lhs = Column(self.lhs.name, table)
        else:
            self.lhs = Column(self.lhs, table)
        if isinstance(self.rhs, Predicate):
            self.rhs.set_table(table)


class Filter:
    """Alternative SQL WHERE clause"""

    OR = ' OR '
    AND = ' AND '

    def __init__(self, preds, op=AND):
        self.preds = preds
        self.op = op
        self.values = ()
        for pred in self.preds:
            self.values += pred.values

    def render(self, placeholder):
        pred_list = []
        for pred in self.preds:
            pred_list.append(pred.render(placeholder))
        result = self.op.join(pred_list)
        if self.op == Filter.OR:
            return f'({result})'
        return result

    def set_table(self, table):
        """Specify table for ALL Predicates in Filter"""
        for pred in self.preds:
            pred.set_table(table)


class Order:
    """SQL ORDER BY clause"""

    ASC = 'ASC'
    DESC = 'DESC'

    def __init__(self, cols):
        self.cols = []
        for col in cols:
            if not isinstance(col, tuple):
                col = (col, Order.ASC)
            if isinstance(col[0], str):
                validate_path(col[0])
            self.cols.append(col)

    def render(self, placeholder):
        col_list = []
        for col in self.cols:
            col_list.append(f'{_quote(col[0])} {col[1]}')
        return ', '.join(col_list)


class Projection:
    """SQL SELECT (really projection - pick column subset) clause"""
    def __init__(self, cols):
        for col in cols:
            _validate_column(col)
        self.cols = cols

    def render(self, placeholder):
        return ', '.join([_quote(col) for col in self.cols])


class Table:
    """SQL Table selection"""

    def __init__(self, name):
        validate_name(name)
        self.name = name

    def render(self, placeholder):
        return self.name


class Group:
    """SQL GROUP clause"""

    def __init__(self, cols):
        for col in cols:
            _validate_column(col)
        self.cols = cols

    def render(self, placeholder):
        cols = []
        for col in self.cols:
            if isinstance(col, Column):  # Again, nasty hacks
                if col.table:
                    cols.append(f'{col.table}"."{col.name}')
                else:
                    cols.append(col.name)
            else:
                cols.append(col)
        return ', '.join([_quote(col) for col in cols])


class Aggregation:
    """Aggregate rows"""

    def __init__(self, aggs):
        self.aggs = []
        for agg in aggs:
            if isinstance(agg, tuple):
                if len(agg) == 3:
                    func, col, alias = agg
                elif len(agg) == 2:
                    func, col = agg
                    alias = None
                if func.upper() not in AGG_FUNCS:
                    raise InvalidAggregateFunction(func)
                if col is not None and col != '*':
                    _validate_column(col)
                self.aggs.append((func, col, alias))
            else:
                raise TypeError('expected aggregation tuple but received ' + str(type(agg)))
        self.group_cols = []  # Filled in by Query

    def render(self, placeholder):
        exprs = [_quote(col) for col in self.group_cols]
        for agg in self.aggs:
            mod = ''
            func, col, alias = agg
            if func.upper() == 'NUNIQUE':
                func = 'COUNT'
                mod = 'DISTINCT '
            if not col:
                col = '*'
            if col == '*':
                expr = f'{func}({mod}{col})'  # No quotes for *
            else:
                expr = f'{func}({mod}{_quote(col)})'
            if not alias:
                alias = func.lower()
            expr += f' AS "{alias}"'
            exprs.append(expr)
        return ', '.join(exprs)


class Offset:
    """SQL row offset"""

    def __init__(self, num):
        self.num = int(num)

    def render(self, placeholder):
        return str(self.num)


class Limit:
    """SQL row count"""

    def __init__(self, num):
        self.num = int(num)

    def render(self, placeholder):
        return str(self.num)


class Count:
    """Count the rows in a result set"""

    def __init__(self):
        pass

    def render(self, placeholder):
        return 'COUNT(*) AS "count"'


class Unique:
    """Reduce the rows in a result set to unique tuples"""

    def __init__(self):
        pass

    def render(self, placeholder):
        return 'SELECT DISTINCT *'


class CountUnique:
    """Unique count of the rows in a result set"""

    def __init__(self, cols=None):
        for col in cols or []:
            _validate_column(col)
        self.cols = cols

    def render(self, placeholder):
        if self.cols:
            cols = ', '.join([f'"{col}"' for col in self.cols])
            return f'COUNT(DISTINCT {cols}) AS "count"'
        return 'COUNT(*) AS "count"'


class Join:
    """Join 2 tables"""

    def __init__(self, name,
                 left_col=None, op=None, right_col=None,
                 preds=None,
                 how='INNER', alias=None, lhs=None):
        """
        Use *either* `left_col`, `op`, and `right_col` or `preds`
        """
        validate_name(name)
        if all((left_col, op, right_col)):
            _validate_column(left_col)
            _validate_column(right_col)
        if alias:
            validate_name(alias)
        if lhs:
            validate_name(name)
        if how.upper() not in JOIN_TYPES:
            raise InvalidJoinOperator(how)
        self.prev_name = lhs  # If none, filled in by Query
        self.name = name
        self.left_col = left_col
        self.op = op
        self.right_col = right_col
        self.how = how
        self.alias = alias
        self.values = tuple()
        self.preds = preds
        if preds:
            for pred in self.preds:
                self.values += pred.values

    def __repr__(self):
        return f'Join({self.name}, {self.left_col}, {self.op}, {self.right_col}, {self.how}, {self.alias}, {self.prev_name})'

    def __eq__(self, rhs):
        return (
            self.prev_name == rhs.prev_name and
            self.name == rhs.name and
            self.left_col == rhs.left_col and
            self.op == rhs.op and
            self.right_col == rhs.right_col and
            self.how == rhs.how and
            self.alias == rhs.alias)

    def render(self, placeholder):
        # Assume there's a FROM before this?
        target = f'"{self.name}"'
        table = target
        if self.alias:
            target += f' AS "{self.alias}"'
            table = f'"{self.alias}"'
        if self.left_col:
            cond = (f'"{self.prev_name}"."{self.left_col}"'
                    f' {self.op} {table}."{self.right_col}"')
        else:
            pred_list = []
            for pred in self.preds:
                tmp = pred.render(placeholder)
                pred_list.append(tmp)
            cond = ' AND '.join(pred_list)
        return f'{self.how.upper()} JOIN {target} ON {cond}'


class Query:
    """
    SQL Query statement

    SQL order of evaluations:
    FROM, including JOINs
    WHERE
    GROUP BY
    HAVING
    WINDOW functions
    SELECT (projection)
    DISTINCT
    UNION
    ORDER BY
    LIMIT and OFFSET
    """
    def __init__(self, arg=None):
        self.table = None
        self.joins = []
        self.where = []
        self.groupby = None
        self.aggs = None
        self.having = []
        #Not supported: windows
        self.proj = None  # Make a list of Projections?
        self.distinct = False
        self.count = False  # FIXME: isn't this an aggregation?
        # TODO: self.union = []
        self.order = None
        self.limit = None
        self.offset = 0
        if isinstance(arg, str):
            self.table = Table(arg)
        elif isinstance(arg, Table):
            self.table = arg
        elif isinstance(arg, list):
            self.extend(arg)

    def append(self, stage):
        if isinstance(stage, Table):
            self.table = stage
        elif isinstance(stage, Join):
            if not self.table:
                raise InvalidQuery('Join must follow Table or Join')
            self.joins.append(stage)
        elif isinstance(stage, Filter):
            if self.groupby:
                self.having.append(stage)
            else:
                self.where.append(stage)
        elif isinstance(stage, Group):
            self.groupby = stage
        elif isinstance(stage, Aggregation):
            # If there's already a Projection, that's an error
            if self.proj:
                raise InvalidQuery('cannot have Aggregation after Projection')
            self.aggs = stage
        elif isinstance(stage, Projection):
            self.proj = stage
        elif isinstance(stage, Count):
            self.count = stage
        elif isinstance(stage, Unique):
            self.distinct = True
        elif isinstance(stage, CountUnique):
            self.count = Count()
            self.distinct = True
        elif isinstance(stage, Order):
            self.order = stage
        elif isinstance(stage, Limit):
            self.limit = stage
        elif isinstance(stage, Offset):
            self.offset = stage
        elif isinstance(stage, Query):
            if not self.table:
                self.table = stage
            #TODO: else?

    def extend(self, stages):
        for stage in stages:
            self.append(stage)

    def render(self, placeholder):
        if not self.table:
            raise InvalidQuery("no table")  #TODO: better message
        result_cols = ''
        sub_count = 0  # Count of "sub queries"
        values = ()
        text = self.table.render(placeholder)
        if isinstance(text, tuple):
            text, values = text
            sub_count += 1
            query = f'FROM ({text}) AS s{sub_count}'
        else:
            query = f'FROM "{text}"'
        for i, join in enumerate(self.joins):
            # prev_name stuff is a hack
            if not join.prev_name:
                join.prev_name = self.table.name if i == 0 else self.joins[i - 1].name
            values += join.values
            text = join.render(placeholder)
            query = f'{query} {text}'
        filts = []
        for filt in self.where:
            filts.append(filt.render(placeholder))
            values += filt.values
        if filts:
            where = ' AND '.join(filts)
            query = f'{query} WHERE {where}'
        if self.groupby:
            text = self.groupby.render(placeholder)
            query = f'{query} GROUP BY {text}'
            # Add group cols to result set automatically
            if result_cols:
                result_cols += ', '
            result_cols += ', '.join([_quote(col) for col in self.groupby.cols])
        filts = []
        for filt in self.having:
            values += filt.values
            filts.append(filt.render(placeholder))
        if filts:
            where = ' AND '.join(filts)
            query = f'{query} HAVING {where}'

        # Projection and Aggregation both add columns to result set
        if self.aggs:
            if result_cols:
                result_cols += ', '
            result_cols += self.aggs.render(placeholder)
        if self.proj:
            if result_cols:
                result_cols += ', '
            result_cols = self.proj.render(placeholder)
        if not result_cols:
            result_cols = '*'

        if self.distinct and self.count and result_cols == '*':
            query = f'COUNT(*) AS "count" FROM (SELECT DISTINCT * {query}) AS tmp'
        elif self.distinct and self.count:
            query = f'COUNT(DISTINCT {result_cols}) AS "count" {query}'
        elif self.distinct:
            query = f'DISTINCT {result_cols} {query}'
        elif self.count:
            query = f'COUNT({result_cols}) AS "count" {query}'
        else:
            query = f'{result_cols} {query}'

        if self.order:
            text = self.order.render(placeholder)
            query = f'{query} ORDER BY {text}'
        if self.limit:
            text = self.limit.render(placeholder)
            query = f'{query} LIMIT {text}'
            if self.offset:
                text = self.offset.render(placeholder)
                query = f'{query} OFFSET {text}'
        query = f'SELECT {query}'
        return query, values
