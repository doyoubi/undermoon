import sys
from enum import Enum
from collections import defaultdict
from copy import deepcopy


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


def check_redirecting(states_tpl):
    m, l, r = states_tpl
    # The latter Queue state is for postpone_redirection_cases
    if m in [MetaState.RedirectToPeer, MetaState.Queue]:
        return True
    if l in [MetaState.Slot, MetaState.MgrSlot, MetaState.IptSlot]:
        return False
    return r in [MetaState.Slot, MetaState.MgrSlot, MetaState.IptSlot]


def check_processing(states_tpl):
    m, l, r = states_tpl
    if m in [MetaState.RedirectToPeer]:
        return False
    if m in [MetaState.Queue]:
        return True
    return l in [MetaState.Slot, MetaState.MgrSlot, MetaState.IptSlot]


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


def gen_path_order_map():
    m = defaultdict(dict)
    for store_row in MetaStore:
        for state_row in MetaState:
            for store_col in MetaStore:
                for state_col in MetaState:
                    m[(store_row, state_row)][(store_col, state_col)] = False

    # Source
    m[(MetaStore.Rs, MetaState.Start)][(MetaStore.Rs, MetaState.IptSlot)] = True
    m[(MetaStore.Rs, MetaState.IptSlot)][(MetaStore.Ms, MetaState.MgrSet)] = True
    m[(MetaStore.Ms, MetaState.MgrSet)][(MetaStore.Ls, MetaState.MgrSlot)] = True

    m[(MetaStore.Ms, MetaState.MgrSet)][(MetaStore.Ms, MetaState.Queue)] = True
    m[(MetaStore.Ms, MetaState.Queue)][(MetaStore.Md, MetaState.RedirectToSelf)] = True

    # Now the coordinator start to commit the switch
    m[(MetaStore.Md, MetaState.RedirectToSelf)][(MetaStore.Ms, MetaState.RedirectToPeer)] = True

    # SETPEER SETDB
    m[(MetaStore.Ms, MetaState.RedirectToPeer)][(MetaStore.Rs, MetaState.Slot)] = True
    m[(MetaStore.Rs, MetaState.Slot)][(MetaStore.Ms, MetaState.End)] = True
    m[(MetaStore.Ms, MetaState.End)][(MetaStore.Ls, MetaState.End)] = True

    # Destination
    m[(MetaStore.Rd, MetaState.Slot)][(MetaStore.Rd, MetaState.MgrSlot)] = True
    m[(MetaStore.Rd, MetaState.MgrSlot)][(MetaStore.Md, MetaState.RedirectToPeer)] = True
    m[(MetaStore.Md, MetaState.RedirectToPeer)][(MetaStore.Ld, MetaState.IptSlot)] = True

    # SETPEER SETDB
    m[(MetaStore.Ms, MetaState.RedirectToPeer)][(MetaStore.Rd, MetaState.End)] = True
    m[(MetaStore.Rd, MetaState.End)][(MetaStore.Md, MetaState.End)] = True
    m[(MetaStore.Md, MetaState.End)][(MetaStore.Ld, MetaState.Slot)] = True

    return m


def need_to_set_Ld_before_commit_cases():
    return [
        (
            MetaState.RedirectToPeer,
            MetaState.Slot,
            MetaState.IptSlot,
            MetaState.RedirectToSelf,
            # need extra codes to make sure before switched to RedirectToSelf,
            # Ls should be already IptSlot
            MetaState.Start,
            MetaState.MgrSlot,
        ),
    ]


def postpone_redirection_cases():
    return [
        (
            MetaState.Queue,
            MetaState.Slot,
            MetaState.IptSlot,
            MetaState.RedirectToSelf,
            MetaState.IptSlot,
            MetaState.MgrSlot,
        ),
    ]


def migration_and_local_meta_not_updated_atomically():
    return [
        (
            MetaState.End,
            MetaState.Slot,
            MetaState.Slot,
            MetaState.RedirectToSelf,
            MetaState.IptSlot,
            MetaState.MgrSlot,
        ),
    ]


def validate_states(states):
    # valid_states = need_to_set_Ld_before_commit_cases()
    # valid_states.extend(postpone_redirection_cases())
    # valid_states.extend(migration_and_local_meta_not_updated_atomically())
    valid_states = []
    for s in valid_states:
        if MetaState.equal_tuple(states, s):
            return True

    mgr_states = (states[MetaStore.Ms], states[MetaStore.Ls], states[MetaStore.Rs])
    ipt_states = (states[MetaStore.Md], states[MetaStore.Ld], states[MetaStore.Rd])
    if check_processing(mgr_states) and check_redirecting(ipt_states):
        return True
    if check_redirecting(mgr_states) and check_processing(ipt_states):
        return True
    return False


def check_order(curr_states, new_store, new_state, m):
    for store, state in curr_states.items():
        if m[(store, state)][(new_store, new_state)]:
            return True
    return False


def get_next_state(orderred_states, store, curr_state):
    states = orderred_states[store]
    for i, state in enumerate(states):
        if state == curr_state:
            if len(states) == i+1:
                return None
            return states[i+1]


def recur_check(curr_states, orderred_states, m):
    next_stores = curr_states.keys()
    for next_store in next_stores:
        next_state = get_next_state(orderred_states, next_store, curr_states[next_store])
        if next_state is None:
            continue

        if not check_order(curr_states, next_store, next_state, m):
            continue

        sts = deepcopy(curr_states)

        sts[next_store] = next_state
        # TODO: need to implement these two rules in the proxy.
        if next_store == MetaStore.Md and next_state == MetaState.RedirectToSelf:
            sts[MetaStore.Ld] = MetaState.IptSlot
        if next_store == MetaStore.Ms and next_state == MetaState.End:
            sts[MetaStore.Ls] = MetaState.End

        if not validate_states(sts):
            print('Invalid States:', next_store, next_state)
            pretty_print_states(sts)
            sys.exit(1)

        recur_check(sts, orderred_states, m)


def print_map(m):
    for row, cols in m.items():
        print(' '.join(list(map(lambda t: 'x' if t else ' ', cols.values()))))


def check():
    path_order_map = gen_path_order_map()
    # print(path_order_map)

    orderred_states = gen_store_order()

    states = {s: MetaState.Start for s in MetaStore}
    states[MetaStore.Ls] = MetaState.Slot
    states[MetaStore.Rd] = MetaState.Slot

    recur_check(deepcopy(states), orderred_states, path_order_map)


def pretty_print_states(states):
    for store, state in states.items():
        print('{},'.format(state))
        # print(store, state)


check()
