# testing

## ProtobufImpl schema change testing

Most of the "simple" protobuf testing is done in the protobuf project, `project(':extensions-protobuf')`. One of
the important parts with respect to the kafka integration is how schema changes are handled, and so that is tested here,
[ProtobufImplSchemaChangeTest.java](src/test/java/io/deephaven/kafka/ProtobufImplSchemaChangeTest.java).

The test development setup for this is a little unconventional due to the fact that protoc won't let you generate
multiple versions of the same message type, at least not within the same protoc invocation. To work around this, there
is a little bit of manual test development workflow needed to add a new message / message version. It requires:
 * Uncommenting the proto plugin logic in [build.gradle](build.gradle)
 * Adding a single new .proto file per version of the message you want to create; it must have `option java_multiple_files = false;` (only one .proto file with a given message name can compile at any given time)
 * Generating the new test code, `./gradlew :extensions-kafka:generateTestProto`
 * Moving the output from [build/generated/source/proto/test/java/io/deephaven/kafka/protobuf/gen](build/generated/source/proto/test/java/io/deephaven/kafka/protobuf/gen) into [src/test/java/io/deephaven/kafka/protobuf/gen](src/test/java/io/deephaven/kafka/protobuf/gen)
 * Renaming the .proto file to .proto.txt (to ensure it doesn't get re-generated later).
 * Commenting out the proto plugin logic in [build.gradle](build.gradle)

While it's likely possible to automate the above, it would likely be a lot of bespoke work that would probably not see
much use elsewhere.
