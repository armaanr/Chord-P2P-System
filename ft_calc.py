import sys

if __name__ == '__main__':
    node = int(sys.argv[1])
    if (len(sys.argv) > 2):
        a = int(sys.argv[2])
        for i in range(8):
            val = a if a >= ((2 ** i) + node) % 256 else 0
            print str(i) + ":" + str(val)
    else:
        for i in range(8):
            val = ((2 ** i) + node) % 256
            print str(i) + ":" + str(val)
