import os
import signal
import sys
import random

from subprocess import Popen, PIPE, STDOUT

#PLIST = [4, 8, 10, 20, 30]
#PLIST = [4]
PLIST = [int(sys.argv[1])]
F = 128
N = 1
INTERPRETER = "java"
EXECUTABLE = "Client"
CLIENT_PORT = "1999"
CONFIG_FILE = "config.txt"
EXEC_STRING = INTERPRETER + " " +  EXECUTABLE + " " +  CLIENT_PORT + " " + CONFIG_FILE
print EXEC_STRING

def main():
    # java Client 1999 config.txt
    for pval in PLIST:
        run_experiment(pval)

def run_experiment(pval):
    for nval in xrange(N):
        random.seed()
        command_list = []
        node_list = [0]
        flist = []
        random_num = random.randint(1,255)
        for p in xrange(pval):
            while (random_num in node_list):
                random_num = random.randint(1,255)
            node_list.append(random_num)
            command_list.append("join " + str(random_num))

        random_key = random.randint(0,255)
        for i in xrange(F):
            while (random_key in flist):
                random_key = random.randint(0,255)
            flist.append(random_key)
            random_node = random.randint(0,pval)
            command_list.append("find " + str(node_list[random_node]) + " " + str(random_key))
#        command_list.append("exit")
        new_trial(command_list)

def new_trial(command_list):
#    exit_val = subprocess.call(exec_string, shell=True)

#    p = Popen(['java', 'Client', '1999', 'config.txt'], stdout=PIPE, stdin=PIPE, stderr=PIPE)

#    all_commands = "\n".join(command_list)

    with open("commands.txt", "w") as f:
        for command in command_list:
            command += '\n'
            f.write(command)
#            p.stdin.write(command)
#        stdout_data = p.communicate(input=command)
#        print stdout_data
#        # write command to stdin in of process

#    stdout_data = p.communicate(input=all_commands)[0]
#    print stdout_data
#    p.send_signal(signal.SIGINT)

if __name__ == '__main__':
    main()
