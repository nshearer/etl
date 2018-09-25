
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
