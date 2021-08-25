package io.deephaven.util.text;

import io.deephaven.base.verify.Require;

/**
 * Simple utility class for managing the indentation of generated code.
 * <p>
 * Pass the indenter to your StringBuilder; calling increaseLevel or decreaseLevel as you start and finish your
 * indentation blocks.
 */
public class Indenter {

    private static final String ONE_LEVEL_INDENT = "    ";
    private String indentString;
    private int level;

    public Indenter() {
        this.indentString = ONE_LEVEL_INDENT;
        this.level = 1;
    }

    public Indenter(final int level) {
        this();
        while (this.level < level) {
            increaseLevel();
        }
        while (this.level > level) {
            decreaseLevel();
        }
    }

    public Indenter increaseLevel() {
        indentString += ONE_LEVEL_INDENT;
        level++;
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public Indenter increaseLevel(final int levels) {
        for (int ii = 0; ii < levels; ++ii) {
            increaseLevel();
        }
        return this;
    }

    public Indenter decreaseLevel() {
        Require.geq(level, "level", 1, "1");
        indentString = indentString.substring(0, indentString.length() - ONE_LEVEL_INDENT.length());
        level--;
        return this;
    }

    @SuppressWarnings("unused")
    public Indenter decreaseLevel(final int levels) {
        for (int ii = 0; ii < levels; ++ii) {
            decreaseLevel();
        }
        return this;
    }

    public StringBuilder indent(StringBuilder destination, String block) {
        String[] lines = block.split("\n");
        int length = lines.length;
        if ((length > 0) && lines[length - 1].length() == 0) {
            length--;
        }
        for (int i = 0; i < length; i++) {
            destination.append(indentString).append(lines[i]).append("\n");
        }
        return destination;
    }

    @Override
    public String toString() {
        return indentString;
    }
}
