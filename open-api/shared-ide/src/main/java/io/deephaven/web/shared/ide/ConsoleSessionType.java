package io.deephaven.web.shared.ide;

/**
 * What type of console script session to start.
 */
public enum ConsoleSessionType {
    Python, Groovy;

    public static ConsoleSessionType from(String type) {
        switch (type.toLowerCase()) {
            case "python":
                return Python;
            case "groovy":
                return Groovy;
        }
        throw new UnsupportedOperationException(
            "Session type " + type + " not supported; valid options are ['groovy' or 'python']");
    }
}
