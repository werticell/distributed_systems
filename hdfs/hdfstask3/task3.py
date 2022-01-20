import subprocess as sp
import re
import sys

# programm takes filename as a command-line argument 

blocks_info = sp.check_output(['hdfs', 'fsck', sys.argv[1], '-blocks'])

valid_block_str = re.search(rb'Total blocks \(validated\):\s*\d+\s*\(', blocks_info)

blocks_count = re.search(rb'\d+', valid_block_str.group(0)).group(0)
print(blocks_count.decode('utf-8'))

