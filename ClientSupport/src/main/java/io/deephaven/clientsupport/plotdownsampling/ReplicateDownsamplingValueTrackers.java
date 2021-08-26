package io.deephaven.clientsupport.plotdownsampling;

import io.deephaven.compilertools.ReplicatePrimitiveCode;

import java.io.IOException;

/**
 * Generates primitive value trackers from the char variant, so that only char and Object need to be manually
 * maintained. When these are changed, please run `./gradlew replicateDownsampleValueTrackers` to regenerate the other
 * types.
 */
public class ReplicateDownsamplingValueTrackers extends ReplicatePrimitiveCode {
    public static void main(String[] args) throws IOException {
        charToAllButBoolean(CharValueTracker.class, MAIN_SRC);
    }
}
