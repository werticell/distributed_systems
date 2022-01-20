import subprocess as sp
import sys 

# programm takes filename as a command-line argument

request = 'http://mipt-master.atp-fivt.org:50070/webhdfs/v1{}?op=OPEN&length=10'.format(sys.argv[1])
response = sp.check_output(["curl", "-L", request])
print(response.decode('utf-8'))

