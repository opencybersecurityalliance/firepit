import logging
from collections import defaultdict

from anytree import Node, PreOrderIter

from firepit.props import get_last, ref_type
from firepit.query import CoalescedColumn, Column, Filter, Join, Predicate, Projection, Query, Table, Unique


logger = logging.getLogger(__name__)


def _make_join(store, lhs, ref, rhs, path, proj):
    # Use the `ref` prop as the alias for table `rhs`
    # Important because e.g. network-traffic needs to JOIN ipv4-addr twice
    alias = '.'.join(path).replace('.', '__')
    proj.extend(
        [
            Column(c, alias, ".".join(path + [c]))
            for c in store.columns(rhs)
            if c != ref and not c.endswith('_ref')
        ]
    )
    return Join(rhs, ref, "=", "id", how="LEFT OUTER", alias=alias, lhs=lhs)


def _join_ip_tables(store, qry, path, proj, prop, prev_table):
    # Special case for when we have BOTH IPv4 and IPv6
    prefix = ".".join(path)
    for n in (4, 6):
        # Join each ip table, and alias it as {prop}4 or {prop}6
        qry.append(
            Join(
                f"ipv{n}-addr",
                prop,
                "=",
                "id",
                how="LEFT OUTER",
                alias=f"{prop}{n}",
                lhs=prev_table,
            )
        )
    v4_cols = set(store.columns("ipv4-addr"))
    v6_cols = set(store.columns("ipv6-addr"))
    # Coalesce columns that are common to both
    for c in v4_cols & v6_cols:
        if c != prop and not c.endswith('_ref'):
            names = [f"{prop}{n}.{c}" for n in (4, 6)]
            proj.append(CoalescedColumn(names, f"{prefix}.{c}"))
    # Collect columns that are exclusive to one table or the other
    for c in v4_cols - v6_cols:
        if c != prop and not c.endswith('_ref'):
            for a in ("src4", "dst4"):
                proj.append(Column(c, a, f"{prefix}.{c}"))
    for c in v6_cols - v4_cols:
        if c != prop and not c.endswith('_ref'):
            for a in ("src6", "dst6"):
                proj.append(Column(c, a, f"{prefix}.{c}"))


def _get_reflists(store, view):
    otype = store.table_type(view) or view
    qry = Query([
        Table('__reflist'),
        Filter([Predicate('source_ref', 'LIKE', f'{otype}--%')]),
        Projection(['ref_name']),
        Unique()
    ])
    print(qry.render('{}'))
    return [r['ref_name'] for r in store.run_query(qry).fetchall()]


def auto_deref(store, view):
    """Automatically resolve all refs for backward compatibility"""
    proj = []
    cols = store.columns(view)
    if 'id' not in cols:
        # view is probably an aggregate; bail
        return [], None
    for col in cols:
        if not col.endswith("_ref"):
            proj.append(Column(col, view))
    all_types = set(store.types())
    mixed_ips = ('ipv4-addr' in all_types and 'ipv6-addr' in all_types)
    root = _dfs(store, view, all_types=all_types)
    #print(RenderTree(root))
    joins = []
    aliases = {}
    for node in PreOrderIter(root):
        if node.parent:
            path = [n.edge for n in node.path if n.edge]
            parent = aliases.get(node.parent.name, node.parent.name)
            aliases[node.name] = '.'.join(path).replace('.', '__')
            if mixed_ips and node.name.startswith("ipv"):
                # special case for concurrent ipv4 and 6
                _join_ip_tables(store, joins, path, proj, node.edge, parent)
            else:
                joins.append(_make_join(store, parent, node.edge, node.name, path, proj))
        if node.name == 'process' and 'parent_ref' in store.columns('process'):
            # special case for process:parent_ref
            path = [n.edge for n in node.path if n.edge] + ['parent_ref']
            parent = '.'.join(path).replace('.', '__')
            alias = aliases.get('process', node.edge)
            # This sets up the projection but gets the JOIN wrong
            _make_join(store, parent, 'parent_ref', 'process', path, proj)
            joins.append(Join('process', 'parent_ref', '=', 'id',
                              how='LEFT OUTER', alias=parent, lhs=alias))

    # Only handle reflists for root node?
    #reflists = _get_reflists(store, view)
    #for reflist in reflists:

    if proj:
        proj = Projection(proj)
    return joins, proj


def _dfs(store, sco_type, parent=None, ref=None, all_types=None):
    """Depth-first search for reference dependencies"""
    node = Node(sco_type, parent=parent, edge=ref)
    props = store.columns(sco_type)
    ignore = set()
    if 'process_ref' in props and 'parent_process_ref' in props:
        # Special case for x-oca-event
        ignore.add('parent_process_ref')
    for prop in props:
        if prop.endswith("_ref") and prop not in ignore:
            rtypes = set(ref_type(sco_type, get_last(prop))) & all_types
            ptype = list(rtypes)[0]
            if ptype != sco_type:
                _dfs(store, ptype, parent=node, ref=prop, all_types=all_types)
    return node


def unresolve(objects):
    """Do the opposite of auto_deref: split out reference objects"""
    assert isinstance(objects, list)
    for obj in objects:
        assert isinstance(obj, dict)
        pruned = {}
        reffed = defaultdict(dict)
        for prop in sorted(obj):
            if '_ref.' in prop:
                # Split off the first part (e.g. src_ref)
                ref, _, rest = prop.partition('.')

                # Add prop to new obj
                reffed[ref][rest] = obj[prop]
            else:
                pruned[prop] = obj[prop]
        for new_obj in reffed.values():
            # Deduce type
            if 'id' in new_obj and new_obj['id']:
                otype, _, _ = new_obj['id'].partition('--')
                new_obj['type'] = otype
                yield from unresolve([new_obj])
        yield pruned
