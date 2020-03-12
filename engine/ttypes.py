#
# Autogenerated by Thrift Compiler (0.13.0)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py
#

from thrift.Thrift import TType, TMessageType, TFrozenDict, TException, TApplicationException
from thrift.protocol.TProtocol import TProtocolException
from thrift.TRecursive import fix_spec

import sys

from thrift.transport import TTransport
all_structs = []


class UserProfile(object):
    """
    Attributes:
     - userId
     - movieWatchedNumRecently

    """


    def __init__(self, userId=None, movieWatchedNumRecently=None,):
        self.userId = userId
        self.movieWatchedNumRecently = movieWatchedNumRecently

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.userId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.I32:
                    self.movieWatchedNumRecently = iprot.readI32()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('UserProfile')
        if self.userId is not None:
            oprot.writeFieldBegin('userId', TType.I64, 1)
            oprot.writeI64(self.userId)
            oprot.writeFieldEnd()
        if self.movieWatchedNumRecently is not None:
            oprot.writeFieldBegin('movieWatchedNumRecently', TType.I32, 2)
            oprot.writeI32(self.movieWatchedNumRecently)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(UserProfile)
UserProfile.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'userId', None, None, ),  # 1
    (2, TType.I32, 'movieWatchedNumRecently', None, None, ),  # 2
)
fix_spec(all_structs)
del all_structs