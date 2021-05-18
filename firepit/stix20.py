import os

from lark import Lark, Transformer, v_args


def get_grammar():
    pth = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "paramstix.lark")
    return open(pth, "r").read()


def stix2sql(pattern, sco_type):
    grammar = get_grammar()
    return Lark(grammar,
                parser="lalr",
                transformer=_TranslateTree(sco_type)).parse(pattern)


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

    def comp_exp(self, lhs, op, rhs):
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
