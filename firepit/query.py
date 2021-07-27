"""Utilities for generating SQL while avoiding SQL injection vulns"""

import re

COMP_OPS = ['=', '<>', '!=', '<', '>', '<=', '>=', 'LIKE', 'IN', 'IS', 'IS NOT']
PRED_OPS = ['AND', 'OR']
AGG_FUNCS = ['COUNT', 'SUM', 'MIN', 'MAX', 'AVG']


class InvalidComparisonOperator(Exception):
    pass


class InvalidPredicateOperator(Exception):
    pass


class InvalidAggregateFunction(Exception):
    pass


class InvalidQuery(Exception):
    pass


class Predicate:
    """Simple row value predicate"""

    def __init__(self, lhs, op, rhs):
        if op not in COMP_OPS:
            raise InvalidComparisonOperator(op)
        if lhs.endswith('[*]'):  # STIX list property
            lhs = lhs[:-3]
            if rhs.lower() != 'null':
                rhs = f"%{rhs}%"  # wrap with SQL wildcards since list is encoded as string
                if op == '=':
                    op = 'LIKE'
                elif op == '!=':
                    op = 'NOT LIKE'
        self.lhs = lhs
        self.op = op
        self.rhs = rhs
        if self.rhs in ['null', 'NULL']:
            self.values = ()
        elif isinstance(self.rhs, (list, tuple)):
            self.values = tuple(self.rhs)
        else:
            self.values = (self.rhs, )

    def render(self, placeholder):
        if self.rhs in ['null', 'NULL']:
            if self.op in ['!=', '<>']:
                text = f'("{self.lhs}" IS NOT NULL)'
            elif self.op == '=':
                text = f'("{self.lhs}" IS NULL)'
            else:
                raise InvalidComparisonOperator(self.op)
        elif self.op == 'IN':
            phs = ', '.join([placeholder] * len(self.rhs))
            text = f'("{self.lhs}" {self.op} ({phs}))'
        else:
            text = f'("{self.lhs}" {self.op} {placeholder})'
        return text


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


class Order:
    """SQL ORDER BY clause"""

    ASC = 'ASC'
    DESC = 'DESC'

    def __init__(self, cols):
        self.cols = []
        for col in cols:
            if isinstance(col, tuple):
                self.cols.append(col)
            elif isinstance(col, str):
                self.cols.append((col, Order.ASC))

    def render(self, placeholder):
        col_list = []
        for col in self.cols:
            col_list.append(f'"{col[0]}" {col[1]}')
        return ', '.join(col_list)


class Projection:
    """SQL SELECT (really projection - pick column subset) clause"""

    def __init__(self, cols):
        self.cols = cols

    def render(self, placeholder):
        return ', '.join([f'"{col}"' for col in self.cols])


class Table:
    """SQL Table selection"""

    def __init__(self, name):
        self.name = name

    def render(self, placeholder):
        return self.name


class Group:
    """SQL GROUP clause"""

    def __init__(self, cols):
        self.cols = cols

    def render(self, placeholder):
        return ', '.join([f'"{col}"' for col in self.cols])


class Aggregation:
    """Aggregate after a Group"""

    def __init__(self, aggs):
        self.aggs = []
        for agg in aggs:
            if isinstance(agg, tuple):
                if len(agg) == 3:
                    func, col, alias = agg
                elif len(agg) == 2:
                    func, col = agg
                    alias = None
                if func not in AGG_FUNCS:
                    raise InvalidAggregateFunction(func)
                self.aggs.append((func, col, alias))
            else:
                raise TypeError('expected aggregation tuple but received ' + str(type(agg)))
        self.group_cols = []  # Filled in by Query

    def render(self, placeholder):
        exprs = [f'"{col}"' for col in self.group_cols]
        for agg in self.aggs:
            func, col, alias = agg
            if not col:
                col = '*'
            if col == '*':
                expr = f'{func}({col})'  # No quotes for *
            else:
                expr = f'{func}("{col}")'
            if not alias:
                alias = func.lower()
            expr += f' AS "{alias}"'
            exprs.append(expr)
        return ', '.join(exprs)


class Offset:
    """SQL row offset"""

    def __init__(self, num):
        self.text = f'{num}'

    def render(self, placeholder):
        return self.text


class Limit:
    """SQL row count"""

    def __init__(self, num):
        self.text = f'{num}'

    def render(self, placeholder):
        return self.text


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
        self.cols = cols

    def render(self, placeholder):
        if self.cols:
            cols = ', '.join([f'"{col}"' for col in self.cols])
            return f'COUNT(DISTINCT {cols}) AS "count"'
        return 'COUNT(*) AS "count"'


class Join:
    """Join 2 tables"""

    def __init__(self, name, left_col, op, right_col, how='INNER'):
        self.prev_name = None
        self.name = name
        self.left_col = left_col
        self.op = op
        self.right_col = right_col
        self.how = how

    def render(self, placeholder):
        # Assume there's a FROM before this?
        return (f'{self.how.upper()} JOIN "{self.name}"'
                f' ON "{self.prev_name}"."{self.left_col}"'
                f' {self.op} "{self.name}"."{self.right_col}"')


class Query:
    def __init__(self):
        self.stages = []

    def append(self, stage):
        if isinstance(stage, Aggregation):
            # If there's already a Projection, that's an error
            for prev in self.stages:
                if isinstance(prev, Projection):
                    raise InvalidQuery('cannot have Aggregation after Projection')
            if self.stages:
                last = self.stages[-1]
                if isinstance(last, Group):
                    stage.group_cols = last.cols  # Copy grouped columns
        elif isinstance(stage, Join):
            # Need to look back and grab previous table name
            last = self.stages[-1] if self.stages else None
            if isinstance(last, (Table, Join)):
                stage.prev_name = last.name
            else:
                raise InvalidQuery('Join must follow Table or Join')
        elif isinstance(stage, Count):
            # See if we can combine with previous stages
            last = self.stages[-1]
            if isinstance(last, Unique):
                self.stages.pop(-1)
                cols = None
                if self.stages:
                    last = self.stages[-1]
                    if isinstance(last, Projection):
                        proj = self.stages.pop(-1)
                        cols = proj.cols
                stage = CountUnique(cols)
        elif isinstance(stage, CountUnique):
            # See if we can combine with previous stages
            last = self.stages[-1]
            if isinstance(last, Projection):
                self.stages.pop(-1)
                stage.cols = last.cols
        self.stages.append(stage)

    def render(self, placeholder):
        # TODO: detect missing table
        query = ''
        values = ()
        prev = None  # TODO: Probably need state machine here
        for stage in self.stages:
            text = stage.render(placeholder)
            if isinstance(stage, Table):
                query = f'FROM "{text}"'
            elif isinstance(stage, Projection):
                query = f'SELECT {text} {query}'
            elif isinstance(stage, Filter):
                values += stage.values
                if isinstance(prev, Aggregation):
                    keyword = 'HAVING'
                elif isinstance(prev, Filter):
                    keyword = 'AND'
                else:
                    keyword = 'WHERE'
                query = f'{query} {keyword} {text}'
            elif isinstance(stage, Group):
                query = f'{query} GROUP BY {text}'
            elif isinstance(stage, Aggregation):
                query = f'SELECT {text} {query}'
            elif isinstance(stage, Order):
                query = f'{query} ORDER BY {text}'
            elif isinstance(stage, Limit):
                query = f'{query} LIMIT {text}'
            elif isinstance(stage, Offset):
                query = f'{query} OFFSET {text}'
            elif isinstance(stage, Count):
                if isinstance(prev, Unique):
                    # Should have already been combined, so wwe should never hit this
                    query = f'SELECT {text} FROM ({query}) AS tmp'
                else:
                    query = f'SELECT {text} {query}'
            elif isinstance(stage, Unique):
                if isinstance(prev, Projection):
                    query = re.sub(r'^SELECT ', 'SELECT DISTINCT ', query)
                else:
                    query = f'SELECT DISTINCT * {query}'
            elif isinstance(stage, Join):
                query = f'{query} {text}'
            elif isinstance(stage, CountUnique):
                if stage.cols:
                    query = f'SELECT {text} {query}'
                else:
                    query = f'SELECT {text} FROM (SELECT DISTINCT * {query}) AS tmp'
            prev = stage
        # If there's no projection...
        if not query.startswith('SELECT'):  # Hacky
            query = 'SELECT * ' + query
        return query, values
