package io.deephaven.extensions.barrage;

public enum ColumnConversionMode {
    // @formatter:offf
    Stringify,
    JavaSerialization,
    ThrowError;
    // @formatter:on

    static ColumnConversionMode conversionModeFbToEnum(final byte mode) {
        switch (mode) {
            case io.deephaven.barrage.flatbuf.ColumnConversionMode.Stringify:
                return ColumnConversionMode.Stringify;
            case io.deephaven.barrage.flatbuf.ColumnConversionMode.JavaSerialization:
                return ColumnConversionMode.JavaSerialization;
            case io.deephaven.barrage.flatbuf.ColumnConversionMode.ThrowError:
                return ColumnConversionMode.ThrowError;
            default:
                throw new UnsupportedOperationException("Unexpected column conversion mode " + mode + " (byte)");
        }
    }

    static byte conversionModeEnumToFb(final ColumnConversionMode mode) {
        switch (mode) {
            case Stringify:
                return io.deephaven.barrage.flatbuf.ColumnConversionMode.Stringify;
            case JavaSerialization:
                return io.deephaven.barrage.flatbuf.ColumnConversionMode.JavaSerialization;
            case ThrowError:
                return io.deephaven.barrage.flatbuf.ColumnConversionMode.ThrowError;
            default:
                throw new UnsupportedOperationException("Unexpected column conversion mode " + mode + " (enum)");
        }
    }
}
