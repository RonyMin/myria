#!/usr/bin/python           # This is server.py file

import socket               # Import socket module
import sys
import struct
import cPickle
import os
import sys
import cloud

from MyriaPythonUDF.serializers import read_int, write_int,SpecialLengths,write_with_length,CloudPickleSerializer

# def sendMsg(sock_file,data):
#     length = len(data)
#     sock_file.write(struct.pack('!i',length))
#     sock_file.write(data)
#     sock_file.flush()


if __name__ == '__main__':
  # Read a local port to connect to from stdin
  java_port = int(sys.stdin.readline())
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  #hostname = socket.gethostname()
  sock.connect(("127.0.0.1",java_port))
  #sock.connect((hostname,12345))

  #f = open("logfile.txt",'wb')

  #infile = os.fdopen(os.dup(sock.fileno()), "rb", 65536)
  outfile = os.fdopen(os.dup(sock.fileno()), "wb", 65536)

  def foo(dt):
      import  numpy as np
      from dipy.denoise import nlmeans
      from dipy.denoise.noise_estimate import estimate_sigma
      print "called!"
      #while True:
        #dt = x.next()
      image = dt[0]
      mask = dt[1]
      sigma = estimate_sigma(image)
      denoised_data = nlmeans.nlmeans(image, sigma=sigma, mask=mask)
      print "finished denoising"
      return denoised_data
        #yield denoised_data



  ser = CloudPickleSerializer()
  #x = ser.dumps(foo)
  ser._write_with_length(foo,outfile)
  #x = cloud.serialization.cloudpickle.dumps(foo)


  #sendMsg(outfile, x)
