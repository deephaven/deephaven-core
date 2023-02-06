This directory contains a vendored version of Flatbuffers v2.0.6,
with the following changes:

1. We have copied only the files we need:
   allocator.h array.h base.h buffer.h buffer_ref.h default_allocator.h
   detached_buffer.h flatbuffer_builder.h flatbuffers.h stl_emulation.h
   string.h struct.h table.h util.h vector_downward.h vector.h verifier.h
2. We have moved the namespace of the code to a Deephaven-internal namespace,
   so as not to conflict with any other flatbuffers being indirectly linked
   from some other library (such as the one inside Arrow).
3. The patch representing step 2 is in the file patch.001 in this directory.


