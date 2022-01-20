import subprocess as sp
import re
import sys

# programm takes block name as a command-line argument 

block_info = sp.check_output(['hdfs', 'fsck', '-blockId', sys.argv[1]])

node_name_match = re.search(rb'Block replica on datanode\/rack:.+\/', block_info)

node_name = node_name_match.group(0).split(rb':')[1].strip(rb'/ ').decode('utf-8')

# blocks are stored lower by fs tree in dfs directory on node
path_in_node_fs = sp.check_output(
    ['sudo', '-u', 'hdfsuser', 'ssh', 'hdfsuser@{}'.format(node_name),
     'find', '/dfs', '-name', sys.argv[1]])

print(node_name, path_in_node_fs.decode('utf-8'), sep=':', end='')

