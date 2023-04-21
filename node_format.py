#!/usr/bin/python3
import re
from typing import List

def _parse_number_range(pattern: str) -> List[str]:
    # example: 003-005
    pair = pattern.split('-')
    width = len(pair[0])
    ret = []
    for i in range(int(pair[0]), int(pair[1])):
        ret.append(str(i).zfill(width))
    ret.append(pair[1])
    return ret


def _parse_node_collection(pattern: str) -> List[str]:
    # example: cas[001,003-005,007]
    idx_left = pattern.find('[')
    prefix = pattern[:idx_left]
    subs = pattern[idx_left+1:len(pattern)-1]
    node_nums = []
    for sub in subs.split(','):
        if '-' in sub:
            node_nums.extend(_parse_number_range(sub))
        else:
            node_nums.append(sub)
    ret = []
    for n in node_nums:
        ret.append(prefix + n)
    return ret

def change_format(unseq_nodelist, debug=False, delimiter=" "):
    node_list = []
    for node_str in unseq_nodelist:
        cur = 0
        while cur < len(node_str):
            node_str = node_str[cur:].lstrip(',')
            # suppose pattern is like 'cas[001,003]'
            match = re.search('^([^\[\],]+\[[^\]]+\])', node_str)
            if match:
                node_list.extend(_parse_node_collection(match.group(1)))
                cur = match.end()
                continue
            # suppose pattern is like 'cas001'
            match = re.search('^([^,]+)', node_str)
            if match:
                node_list.append(match.group(1))
                cur = match.end()
                continue
            # as long as node_str is legal, code will not run to here
            print("illegal string pattern: ", node_str)
            exit(1)
    node_list.sort()
    if debug:
        for i in range(len(node_list)-1):
            print(node_list[i], delimiter, sep='', end='')
        print(node_list[len(node_list)-1], flush=True)
    return node_list

if __name__ == "__main__":
    nodelist = ["node[528-529,540-564]"]
    change_format(nodelist,debug=True)
