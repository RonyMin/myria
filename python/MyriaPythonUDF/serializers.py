import sys
import types
import struct
import cPickle
import cloud
import itertools

#pickling protocol to use
protocol = 2

class SpecialLengths(object):
  END_OF_DATA_SECTION = -1
  PYTHON_EXCEPTION_THROWN = -2
  END_OF_STREAM = -3
  NULL = -5


class PickleSerializer(object):

  def dump_stream(self,iterator,stream):
    for obj in iterator:
      self._write_with_length(obj,stream)


  def load_stream(self,stream, size):
    while True:
      try:
        (yield self._read_with_length(stream, size))
      except EOFError:
        return


  def _write_with_length(self, obj, stream):
      serialized = self.dumps(obj)
      print("serilizedobjec is ready ")
      print(str(len(serialized)))
      if serialized is None:
          raise ValueError("serialized value should not be None")
      if len(serialized)>(1<<31):
          raise ValueError("can not serialize object larger than 2G")

      write_int(len(serialized), stream)
      stream.write(serialized)
      print("wrote the serialized object back")


  def _read_with_length(self,stream, size):
      datalist= []
      for i in range (size):
          length = read_int(stream)
          if length == SpecialLengths.END_OF_DATA_SECTION:
              raise EOFError
          elif length ==SpecialLengths.NULL:
              return None
          obj = stream.read(length)
          if len(obj) < length:
              raise EOFError
          datalist.append(self.loads(obj))
      return datalist

  def _read_command(self,stream):
      length = read_int(stream)
      #print "function object length"
      #print length
      if length == SpecialLengths.END_OF_DATA_SECTION:
          raise EOFError
      elif length == SpecialLengths.NULL:
          return None
      obj = stream.read(length)
      if len(obj) < length:
          raise EOFError
      return self.loads(obj)

  def dumps(self, obj):
      return cPickle.dumps(obj,protocol)

  def loads(self,obj):
      return cPickle.loads(obj)

class CloudPickleSerializer(PickleSerializer):

    def dumps(self, obj):
        return cloud.serialization.cloudpickle.dumps(obj, 2)


def read_int(stream):
    length = stream.read(4)
    if not length:
        raise EOFError
    return struct.unpack("!i",length)[0]

def write_int(value, stream):
    stream.write(struct.pack("!i",value))

def write_with_length(obj, stream):
    write_int(len(obj),stream)
    stream.write(obj)
