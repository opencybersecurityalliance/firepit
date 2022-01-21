import os

from lark import Lark, Transformer, v_args

from firepit.props import parse_path


def get_grammar():
    pth = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "paramstix.lark")
    return open(pth, "r").read()


def stix2sql(pattern, sco_type):
    grammar = get_grammar()
    return Lark(grammar,
                parser="lalr",
                transformer=_TranslateTree(sco_type)).parse(pattern)


def _convert_op(sco_type, prop, op, rhs):
    # TODO: update this
    orig_op = op
    neg, _, op = op.rpartition(' ')
    if op == 'ISSUBSET':
        op = '=='
        if sco_type == 'ipv4-addr' or prop in ['src_ref.value',
                                               'dst_ref.value']:
            return f'{neg} (in_subnet("{prop}", {rhs}))'
        else:
            raise ValueError(
                f'{orig_op} not supported for SCO type {sco_type}')
    elif op == 'ISSUPERSET':  # When would anyone use ISSUPERSET?
        op = '=='
        if sco_type == 'ipv4-addr' or prop in ['src_ref.value',
                                               'dst_ref.value']:
            return f'{neg} (in_subnet({rhs}, "{prop}"))'  # FIXME!
        else:
            raise ValueError(
                f'{orig_op} not supported for SCO type {sco_type}')
    elif 'MATCHES' in op:
        return f'{neg} match({rhs}, "{prop}")'
    return f'"{prop}" {neg} {op} {rhs}'
    ## ORIG:
    """
    neg, sep, op = op.rpartition(' ')
    if op == 'ISSUBSET':
        op = '==' if not neg else '!='
        if sco_type == 'ipv4-addr':
            return f'ip2int("{prop}") & ip2int({rhs}) {op} ip2int({rhs})'
        else:
            raise ValueError(
                f'{op} not supported for SCO type {sco_type}')
    elif op == 'ISSUPERSET':  # When would anyone use ISSUPERSET?
        op = '==' if not neg else '!='
        if sco_type == 'ipv4-addr':
            return f'ip2int("{prop}") & ip2int({rhs}) {op} ip2int("{prop}")'
        else:
            raise ValueError(
                f'{op} not supported for SCO type {sco_type}')
    elif 'MATCHES' in op:
        return f'{neg}{sep}match({rhs}, "{prop}")'
    return f'"{prop}" {neg}{sep}{op} {rhs}'
    """


def comp2sql(sco_type, path, op, value):
    result = ''
    links = parse_path(path)
    for link in reversed(links):
        if link[0] == 'node':
            #result = _convert_op(sco_type, link[2], op, value)
            from_type = link[1] or sco_type
            result = _convert_op(from_type, link[2], op, value)
        elif link[0] == 'rel':
            result = f'"{link[2]}" IN (SELECT "id" FROM "{link[3]}" WHERE {result})'
    return result


def path2sql(sco_type, path):
    result = ''
    links = parse_path(path)
    for link in reversed(links):
        print('path2sql:', link)
        if link[0] == 'node':
            #result = _convert_op(sco_type, link[2], op, value)
            pass
        elif link[0] == 'rel':
            #subquery = f'"{link[2]}" IN (SELECT "id" FROM "{link[3]}"'
            #if result:
            #    subquery += f' WHERE {result})'
            result = f'"{link[2]}" IN (SELECT "id" FROM "{link[3]}" WHERE {result})'
    return result


@v_args(inline=True)
class _TranslateTree(Transformer):
    """Transformer to convert relevant parts of STIX pattern to WHERE clause"""

    def __init__(self, sco_type):
        self.sco_type = sco_type

    def _make_comp(self, lhs, op, rhs):
        orig_op = op
        sco_type, _, prop = lhs.partition(':')

        # Ignore object paths that don't match table type
        if self.sco_type == sco_type:
            return comp2sql(sco_type, prop, op, rhs)
            neg, _, op = op.rpartition(' ')
            if op == 'ISSUBSET':
                op = '=='
                if sco_type == 'ipv4-addr' or lhs in ['network-traffic:src_ref.value',
                                                      'network-traffic:dst_ref.value']:
                    return f'{neg} (in_subnet("{prop}", {rhs}))'
                else:
                    raise ValueError(
                        f'{orig_op} not supported for SCO type {sco_type}')
            elif op == 'ISSUPERSET':  # When would anyone use ISSUPERSET?
                op = '=='
                if sco_type == 'ipv4-addr' or lhs in ['network-traffic:src_ref.value',
                                                      'network-traffic:dst_ref.value']:
                    return f'{neg} (in_subnet({rhs}, "{prop}"))'  # FIXME!
                else:
                    raise ValueError(
                        f'{orig_op} not supported for SCO type {sco_type}')
            elif 'MATCHES' in op:
                return f'{neg} match({rhs}, "{prop}")'
            if prop.endswith('[*]'):
                prop = prop[:-3]
                op = 'LIKE'
                rhs = rhs.strip("'")
                rhs = f"'%{rhs}%'"
                if op == '!=':
                    neg = 'NOT'
            return f'"{prop}" {neg} {op} {rhs}'
        return ''

    def _make_exp(self, lhs, op, rhs):
        return op.join(filter(None, [lhs, rhs]))

    def disj(self, lhs, rhs):
        return self._make_exp(lhs, ' OR ', rhs)

    def conj(self, lhs, rhs):
        return self._make_exp(lhs, ' AND ', rhs)

    def obs_disj(self, lhs, rhs):
        return self.disj(lhs, rhs)

    def obs_conj(self, lhs, rhs):
        return self.conj(lhs, rhs)

    def comp_grp(self, exp):
        return f'({exp})'

    def simple_comp_exp(self, lhs, op, rhs):
        return self._make_comp(lhs, op, rhs)

    def comp_disj(self, lhs, rhs):
        return self.disj(lhs, rhs)

    def comp_conj(self, lhs, rhs):
        return self.conj(lhs, rhs)

    def op(self, value):
        return f'{value}'

    def quoted_str(self, value):
        return f"'{value}'"

    def lit_list(self, *args):
        return "(" + ','.join(args) + ")"

    def start(self, exp, qualifier):
        # For now, drop the qualifier.  Assume the query handled it.
        return f'{exp}'

    def object_path(self, sco_type, prop):
        return f'{sco_type}:{prop}'
