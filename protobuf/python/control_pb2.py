# Generated by the protocol buffer compiler.  DO NOT EDIT!

from google.protobuf import descriptor
from google.protobuf import message
from google.protobuf import reflection
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)



DESCRIPTOR = descriptor.FileDescriptor(
  name='control.proto',
  package='',
  serialized_pb='\n\rcontrol.proto\"\xb2\x01\n\x0e\x43ontrolMessage\x12\x30\n\x04type\x18\x01 \x02(\x0e\x32\".ControlMessage.ControlMessageType\x12\x10\n\x08remoteID\x18\x02 \x01(\x05\"\\\n\x12\x43ontrolMessageType\x12\x0b\n\x07\x43ONNECT\x10\x00\x12\x0f\n\x0bSTART_QUERY\x10\x01\x12\x1a\n\x16QUERY_READY_TO_EXECUTE\x10\x02\x12\x0c\n\x08SHUTDOWN\x10\x03\x42\x34\n$edu.washington.escience.myriad.protoB\x0c\x43ontrolProto')



_CONTROLMESSAGE_CONTROLMESSAGETYPE = descriptor.EnumDescriptor(
  name='ControlMessageType',
  full_name='ControlMessage.ControlMessageType',
  filename=None,
  file=DESCRIPTOR,
  values=[
    descriptor.EnumValueDescriptor(
      name='CONNECT', index=0, number=0,
      options=None,
      type=None),
    descriptor.EnumValueDescriptor(
      name='START_QUERY', index=1, number=1,
      options=None,
      type=None),
    descriptor.EnumValueDescriptor(
      name='QUERY_READY_TO_EXECUTE', index=2, number=2,
      options=None,
      type=None),
    descriptor.EnumValueDescriptor(
      name='SHUTDOWN', index=3, number=3,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=104,
  serialized_end=196,
)


_CONTROLMESSAGE = descriptor.Descriptor(
  name='ControlMessage',
  full_name='ControlMessage',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    descriptor.FieldDescriptor(
      name='type', full_name='ControlMessage.type', index=0,
      number=1, type=14, cpp_type=8, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    descriptor.FieldDescriptor(
      name='remoteID', full_name='ControlMessage.remoteID', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _CONTROLMESSAGE_CONTROLMESSAGETYPE,
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=18,
  serialized_end=196,
)

_CONTROLMESSAGE.fields_by_name['type'].enum_type = _CONTROLMESSAGE_CONTROLMESSAGETYPE
_CONTROLMESSAGE_CONTROLMESSAGETYPE.containing_type = _CONTROLMESSAGE;
DESCRIPTOR.message_types_by_name['ControlMessage'] = _CONTROLMESSAGE

class ControlMessage(message.Message):
  __metaclass__ = reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _CONTROLMESSAGE
  
  # @@protoc_insertion_point(class_scope:ControlMessage)

# @@protoc_insertion_point(module_scope)
