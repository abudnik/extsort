import os
import random


def CheckFileSize():
    s = os.stat("input.txt")
    return s.st_size >= 4 * 1024 * 1024 * 1024 # 4 Gb

def RandBigInt():
    return random.randrange(0, 10000000000000000000000000000)

def main():
    f = open("input.txt", "w")

    i = 0
    while True:
        if ( i > 10000 ):
            if CheckFileSize() == True:
                break
        i += 1

        v = RandBigInt()
        f.write(str(v)+'\n')

    f.close()

    print 'done...'

main()
