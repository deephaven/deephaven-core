package io.deephaven.benchmarking.generator;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.benchmarking.generator.random.ExtendedRandom;

public class CharColumnGenerator implements ColumnGenerator<Character> {
    protected NumGenerator generator = null;
    private final ColumnDefinition<Character> def;
    private final char min, max;

    public CharColumnGenerator(String name, char min, char max) {
        def = ColumnDefinition.ofChar(name);
        this.min = min;
        this.max = max;
    }

    @Override
    public ColumnDefinition<Character> getDefinition() {
        return def;
    }

    @Override
    public String getUpdateString(String varName) {
        return def.getName() + "=(char)" + varName + ".getChar()";
    }

    @Override
    public void init(ExtendedRandom random) {
        generator = new NumGenerator(min, max, random);
    }

    public char getChar() {
        return generator.getChar();
    }

    @Override
    public String getName() {
        return def.getName();
    }
}
