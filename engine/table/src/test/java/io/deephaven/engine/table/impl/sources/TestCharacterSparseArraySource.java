package io.deephaven.engine.table.impl.sources;

import org.jetbrains.annotations.NotNull;

public class TestCharacterSparseArraySource extends AbstractCharacterColumnSourceTest {
    @NotNull
    @Override
    CharacterSparseArraySource makeTestSource() {
        return new CharacterSparseArraySource();
    }
}