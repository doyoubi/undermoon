from enum import Enum
from collections import defaultdict


class MetaStore(Enum):
    Ms = 'Ms'
    Ls = 'Ls'
    Rs = 'Rs'
    Md = 'Md'
    Ld = 'Ld'
    Rd = 'Rd'


class MetaState(Enum):
    Start = 'Start'
    End = 'End'
    Queue = 'Queue'
    RedirectToPeer = 'RedirectToPeer'
    RedirectToSelf = 'RedirectToSelf'
    MgrSet = 'MgrSet'
    Slot = 'Slot'
    MgrSlot = 'MgrSlot'
    IptSlot = 'IptSlot'
    Any = 'Any'

    @classmethod
    def equal_tuple(cls, states, tpl):
        states_tpl = (
            states[MetaStore.Ms],
            states[MetaStore.Ls],
            states[MetaStore.Rs],
            states[MetaStore.Md],
            states[MetaStore.Ld],
            states[MetaStore.Rd],
        )
        for a, b in zip(states_tpl, tpl):
            if not cls.state_eq(a, b):
                return False
        return True

    @classmethod
    def state_eq(cls, s1, s2):
        if MetaState.Any in [s1, s2]:
            return True
        return s1 == s2


assert MetaState.state_eq(MetaState.Start, MetaState.Start)
assert not MetaState.state_eq(MetaState.Start, MetaState.End)
assert MetaState.state_eq(MetaState.Any, MetaState.End)


def get_all_states(states):
    return (
        states[MetaStore.Ms],
        states[MetaStore.Ls],
        states[MetaStore.Rs],
        states[MetaStore.Md],
        states[MetaStore.Ld],
        states[MetaStore.Rd],
    )


def gen_store_order():
    return {
        MetaStore.Ms: [MetaState.Start, MetaState.MgrSet, MetaState.Queue, MetaState.RedirectToPeer, MetaState.End],
        MetaStore.Ls: [MetaState.Slot, MetaState.MgrSlot, MetaState.End],
        MetaStore.Rs: [MetaState.Start, MetaState.IptSlot, MetaState.Slot],
        MetaStore.Md: [MetaState.Start, MetaState.RedirectToPeer, MetaState.RedirectToSelf, MetaState.End],
        MetaStore.Ld: [MetaState.Start, MetaState.IptSlot, MetaState.Slot],
        MetaStore.Rd: [MetaState.Slot, MetaState.MgrSlot, MetaState.End],
    }


def gen_partially_ordered_map():
    m = defaultdict(dict)
    for store_row in MetaStore:
        for state_row in MetaState:
            for store_col in MetaStore:
                for state_col in MetaState:
                    m[(store_row, state_row)][(store_col, state_col)] = False

    basic_order = gen_store_order()
    for store in basic_order.keys():
        for state1 in basic_order[store][:-1]:
            for state2 in basic_order[store][1:]:
                m[(store, state1)][(store, state2)] = True

    # guaranteed by the order of SETPEER, SETDB-MIGRATION, SETDB-LOCAL
    m[(MetaStore.Rs, MetaState.IptSlot)][(MetaStore.Ms, MetaState.MgrSet)] = True
    m[(MetaStore.Ms, MetaState.MgrSet)][(MetaStore.Ls, MetaState.MgrSlot)] = True
    m[(MetaStore.Rd, MetaState.MgrSlot)][(MetaStore.Md, MetaState.RedirectToPeer)] = True
    m[(MetaStore.Md, MetaState.RedirectToPeer)][(MetaStore.Ld, MetaState.IptSlot)] = True

    # by the process of migration
    m[(MetaStore.Ms, MetaState.Queue)][(MetaStore.Md, MetaState.RedirectToSelf)] = True
    m[(MetaStore.Md, MetaState.RedirectToSelf)][(MetaStore.Ms, MetaState.RedirectToPeer)] = True

    return compute_partially_ordered_map(m)


def get_table_true_count(m):
    return len(list(filter(lambda x: x, sum([list(col.values()) for col in m.values()], []))))


def compute_partially_ordered_map(m):
    last_count = get_table_true_count(m)
    while True:
        for row_key, values in m.items():
            for col_key, tag in values.items():
                if not tag:
                    continue
                dst = col_key
                for dst_key, dst_tag in m[dst].items():
                    if dst_tag:
                        m[row_key][dst_key] = True

        count = get_table_true_count(m)
        if count == last_count:
            return m
        last_count = count


def validate_states(states):
    valid_states = [
        (
            MetaState.Start,
            MetaState.Slot,
            MetaState.Any,
            MetaState.Start,
            MetaState.Start,
            MetaState.Slot,
        ),
        (
            MetaState.Start,
            MetaState.MgrSlot,
            MetaState.Any,
            MetaState.Start,
            MetaState.Start,
            MetaState.Slot,
        ),
        (
            MetaState.Start,
            MetaState.MgrSlot,
            MetaState.Any,
            MetaState.Start,
            MetaState.Start,
            MetaState.MgrSlot,
        ),
    ]
    for s in valid_states:
        if MetaState.equal_tuple(states, s):
            return True
    return False


def validate_order(states):
    pass


def check():
    partial_order_map = gen_partially_ordered_map()
    # for row, cols in partial_order_map.items():
    #     print(' '.join(list(map(lambda t: 'x' if t else ' ', cols.values()))))
    states = {s: MetaState.Start for s in MetaStore}
    print(states)

check()
