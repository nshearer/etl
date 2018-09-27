
def ngroup(seq, group_size):
    '''
    Split sequence up into groups

    :param seq: Sequence to get items from
    :param group_size: number of items to yeild at a time
    '''

    group = list()
    for item in seq:
        group.append(item)
        if len(group) >= group_size:
            yield group
            group = list()

    if len(group) > 0:
        yield group


def sort_iters(seq_a, seq_b):
    '''Given two iterators (that are already sorted) return a sorted combine list'''

    # Make sure we have iterators
    seq_a = iter(seq_a)
    seq_b = iter(seq_b)


    # Get first items
    try:
        a = next(seq_a)
    except StopIteration:
        for b in seq_b:
            yield b
        return

    try:
        b = next(seq_b)
    except StopIteration:
        for a in seq_a:
            yield a
        return

    # Start comparing
    if a < b:

        yield a

        try:
            a = next(seq_a)
        except StopIteration:
            for b in seq_b:
                yield b
            return

    else:

        yield b

        try:
            b = next(seq_b)
        except StopIteration:
            for a in seq_a:
                yield a
            return


def unique_sorted(seq):
    '''Return unique list of items assuming sequence is already sorted'''
    last = None
    saw_None = False
    for item in seq:
        if item is not None:
            if last is None or item != last:
                yield item
                last = item
        else:
            saw_None = True

    if saw_None:
        yield None

