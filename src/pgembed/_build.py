"""
dummy module used in setup()
seems needed to cause the binaries to be well formed
The build is done by the Makefile
"""
from cffi import FFI

ffibuilder = FFI()
ffibuilder.set_source("_postgresql", "")
ffibuilder.cdef("")

if __name__ == "__main__":
    ffibuilder.compile(verbose=True)
