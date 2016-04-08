import os
import sys
import time
import socket
import traceback
import numpy as np
from dipy.denoise import nlmeans
from dipy.denoise.noise_estimate import estimate_sigma
import struct
import cPickle



def recvMsg( sock_file):
    lengthbuf = sock_file.read(4)
    length, = struct.unpack('>i',lengthbuf)
    return recvall(length,sock_file)

def sendMsg(sock_file,data):
    length = len(data)
    sock_file.write(struct.pack('>i',length))
    sock_file.write(data)
    sock_file.flush()

def recvall(count, file):
    buf =b''
    buf = file.read(count)
    return buf

if __name__ == '__main__':
    # Read a local port to connect to from stdin
    java_port = int(sys.stdin.readline())
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #hostname = socket.gethostname()
    sock.connect(("127.0.0.1",java_port))
    #sock.connect((hostname,12345))

    #f = open("logfile.txt",'wb')

    infile = os.fdopen(os.dup(sock.fileno()), "rb", 65536)
    outfile = os.fdopen(os.dup(sock.fileno()), "wb", 65536)

    l = recvMsg(infile)
    #f.write("Length file1: "+str(len(l)))
    image = cPickle.loads(l)


    #get second file
    l2 = recvMsg(infile)
    #f.write("\nLength of  file2: "+str(len(l2)))
    mask = cPickle.loads(l2)


    sigma = estimate_sigma(image)
    #f.write("\n got sigma")

    denoised_data = nlmeans.nlmeans(image, sigma=sigma, mask=mask)
    #f.write("\ngot denoised data")

    pic = cPickle.dumps(denoised_data, 1)

    #f.write("\nWrote denoised image file back,  ints: "+ str(len(pic)))

    sendMsg(outfile,pic)
    #sock.close
    #f.flush()
    #f.close()
    #print 'done sending stuff'
