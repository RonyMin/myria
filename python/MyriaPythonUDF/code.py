#!/usr/bin/python           # This is server.py file

import socket               # Import socket module
import sys
import os
import numpy as np
from dipy.denoise import nlmeans
from dipy.denoise.noise_estimate import estimate_sigma
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
  outfile = os.fdopen(os.dup(sock.fileno()), "wb", 65536)

  def foo(dt):
      image = dt[0]
      mask = dt[1]
      sigma = estimate_sigma(image)
      denoised_data = nlmeans.nlmeans(image, sigma=sigma, mask=mask)
      return denoised_data




  ser = CloudPickleSerializer()
  ser.write_with_length(foo,outfile)



  #sendMsg(outfile, x)
