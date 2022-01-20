import subprocess as sp
import re
import sys

# programm takes file size as a command-line argument 


def create_file_hdfs(size: str, filename: str = 'tmp.txt'):
    name_in_hdfs = '/tmp/{}'.format(filename) # no right to write to root directory
    sp.check_call(['dd', 'if=/dev/zero', 'of={}'.format(filename), 'bs={}'.format(size), 'count=1'])
    sp.check_call(['hdfs', 'dfs', '-put', '-l','-f', filename, name_in_hdfs])
    sp.check_call(['rm', filename])
    sp.check_call(['sleep', '5s'])
    return name_in_hdfs


def get_node_address(blockid: str) -> str:
    block_info = sp.check_output(['hdfs', 'fsck', '-blockId', blockid]).decode('utf-8')
    node_address_match = re.search('Block replica on datanode\/rack:.+\/', block_info)
    node_address = node_address_match.group(0).split(':')[1].strip('/ ')
    # print(node_address)
    return node_address


def real_block_size(node_addr: str, blockid: str):
    filepath = sp.check_output(['sudo', '-u', 'hdfsuser', 'ssh',
                'hdfsuser@{}'.format(node_addr), 'find', '/dfs', '-name', blockid]).decode('utf-8')
    stats_str = sp.check_output(['sudo', '-u', 'hdfsuser', 'ssh',
                'hdfsuser@{}'.format(node_addr), 'stat', filepath]).decode('utf-8')
    # print(stats_str)
    match = re.search('Size:\s*\d+|Размер:\s*\d+', stats_str).group(0)
    return int(re.search('\d+', match).group(0))


filename = create_file_hdfs(sys.argv[1])

blocks_info = sp.check_output(['hdfs', 'fsck', filename, '-files', '-blocks', '-locations']).decode('utf-8')
blockid_matches = re.findall('blk_\d+', blocks_info)

overall_size = 0
for blockid in blockid_matches:
    node_addr = get_node_address(blockid)
    overall_size += real_block_size(node_addr, blockid)

print(overall_size - int(sys.argv[1]))
sp.check_call(['hdfs', 'dfs', '-rm',filename])

