import subprocess as sp
import re
import sys

# programm takes filename as a command-line argument 

blocks_info = sp.check_output(['hdfs', 'fsck', sys.argv[1], '-files', '-blocks', '-locations'])

datanode_info = re.search(rb'DatanodeInfoWithStorage\[.+?,.+?,.+?\]', blocks_info)

node_params = datanode_info.group(0)[len('DatanodeInfoWithStorage'):]
node_ip = node_params.strip(rb'[]').split(rb',')[0].split(rb':')[0]
print(node_ip.decode('utf-8'))

