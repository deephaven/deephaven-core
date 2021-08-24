package io.deephaven.parquet;

import io.deephaven.base.Pair;
import org.apache.parquet.schema.Type;

import java.nio.IntBuffer;
import java.util.Arrays;

/**
 * Provides logic for an arbitrarily nested leaf in the tree. The implementation allows for both
 * repeating and optional levels. The required levels are being ignored as they are implicitly
 * present OR the controller should not be used for leaves where the entire path it required (in
 * that case we have a simple straight copy of column data to the leaf array)
 */
class LevelsController {


    private final Level[] levelsList;

    private final int[] repeatLevelToDefLevel;

    LevelsController(Type.Repetition... levels) {
        Type.Repetition[] newLevels = new Type.Repetition[levels.length + 1];
        newLevels[0] = Type.Repetition.REPEATED;
        System.arraycopy(levels, 0, newLevels, 1, levels.length);
        levels = newLevels;
        levelsList = new Level[levels.length];
        repeatLevelToDefLevel = new int[(int) Arrays.stream(levels)
            .filter(levelType -> levelType == Type.Repetition.REPEATED).count()];
        int rlPos = 0;
        for (int i = 0; i < levels.length; i++) {
            levelsList[i] = buildLevel(levels[i]);
            if (levels[i] == Type.Repetition.REPEATED) {
                repeatLevelToDefLevel[rlPos++] = i;
            }
        }
        levelsList[0].addElements(1);// This is the root schema element and it needs one and only
                                     // entry
    }

    private Level buildLevel(Type.Repetition level) {
        switch (level) {
            case OPTIONAL:
                return new OptionalLevel();
            case REPEATED:
                return new RepeatingLevel();
            default:
                throw new UnsupportedOperationException("No need for processing " + level);
        }
    }

    void addElements(int repeatLevel, int defLevel, int elementsCount) {
        int correspondingDefLevel = repeatLevelToDefLevel[repeatLevel];
        levelsList[correspondingDefLevel].addValues(elementsCount);


        /*
         * reason around something that has 3 nested repeating V DL RL {} 0 0 {{}} 1 0 {{{}}} 2 0 {}
         * 0 0 {} 0 0 {{} 1 0 {}} 1 1 {{{} 2 0 {}}} 2 2 {{{1 3 0 2} 3 3 {} 2 2 {1 3 2 2 3 3 3}}} 3 3
         * 
         * addElements(0,0,1): levels[0].addValues(1) -> l[0] = {1},cvc=1 levels[1].addNulls(1) ->
         * l[1] = {0}, cvc=0
         * 
         * addElements(1,0,1): levels[0].addValues(1) -> l[0] = {2},cvc=2 levels[1].addElement(1) ->
         * l[1] = {0,1}, cvc = 1 levels[2].addNull() -> l[2] = {0}, cvc = 0
         * 
         * addElements(2,0,1): levels[0].addValues(1) -> l[0] = {3},cvc=3 levels[1].addElement(1) ->
         * l[1] = {0,1,2}, cvc = 2 levels[2].addElement(1) -> l[2] = {0,1}, cvc = 1
         * levels[3].addNull() -> l[3] = {0}, cvc = 0
         * 
         * addElements(0,0,2): levels[0].addValues(2) -> l[0] = {5},cvc=5 levels[1].addNulls(2) ->
         * l[1] = {0,1,2,2,2}, cvc=2 addElements(1,0,1): levels[0].addValues(1) -> l[0] = {6},cvc=6
         * levels[1].addElement(1) -> l[1] = {0,1,2,2,2,3}, cvc = 3 levels[2].addNull() -> l[2] =
         * {0,1,1}, cvc = 1 addElements(1,1,1): levels[1].addValues(1) -> l[1] = {0,1,2,2,2,4}, cvc
         * = 4 levels[2].addNull() -> l[2] = {0,1,1,1}, cvc = 1 addElements(2,0,1):
         * levels[0].addValues(1) -> l[0] = {7},cvc=7 levels[1].addElement(1) -> l[1] =
         * {0,1,2,2,2,4,5}, cvc = 5 levels[2].addElement(1) -> l[2] = {0,1,1,1,2}, cvc = 2
         * levels[3].addNull() -> l[3] = {0,0}, cvc = 0 addElements(2,2,1): levels[2].addValues(1)
         * -> l[2] = {0,1,1,1,3}, cvc = 3 levels[3].addNull() -> l[3] = {0,0,0}, cvc = 0
         * addElements(3,0,1): levels[0].addValues(1) -> l[0] = {8},cvc=8 levels[1].addElement(1) ->
         * l[1] = {0,1,2}, cvc = 2 levels[2].addElement(1) -> l[2] = {0,1,1,1,2,3}, cvc = 3
         * levels[3].addElement(1) -> l[3] = {0,0,0,1}, cvc = 1 addElements(3,3,1):
         * levels[3].addValues(1) -> l[3] = {0,0,0,2}, cvc = 2 addElements(2,2,1):
         * levels[2].addValues(1) -> l[2] = {0,1,1,1,3}, cvc = 3 levels[3].addNull() -> l[3] =
         * {0,0,0,2,0}, cvc = 2 addElements(3,2,1): levels[2].addValues(1) -> l[2] = {0,1,1,1,4},
         * cvc = 4 levels[3].addElement(1) -> l[3] = {0,0,0,2,0,3}, cvc = 3 addElements(3,3,2):
         * levels[3].addValues(2) -> l[3] = {0,0,0,2,0,5}, cvc = 5
         * 
         * 
         */
        /*
         * reason around something that has 3 nested optionals V DL RL null 0 0 a.null 1 0 a.b.null
         * 2 0 null 0 0 null 0 0 a.null 1 0 a.null 1 0 a.b.null 2 0 a.b.null 2 0 a.b.c 3 0 a.b.c 3 0
         * a.b.null 2 0 a.b.c 3 0 a.b.c 3 0 a.b.c 3 0
         * 
         * addElements(0,0,1): levels[0].addValues(1) -> l[0] = {1},cvc=1 levels[1].addNulls(1) ->
         * l[1] = {0}, cvc=1 addElements(1,0,1): levels[0].addValues(1) -> l[0] = {2},cvc=2
         * levels[1].addElements(1) -> l[1]={0} cvc = 2 levels[2].addNulls(1) -> l[2] = {0}, cvc=1
         * addElements(2,0,1): levels[0].addValues(1) -> l[0] = {3},cvc=3 levels[1].addElements(1)
         * -> l[1]={0} cvc = 3 levels[2].addElements(1) -> l[2]={0} cvc = 2 levels[3].addNulls(1) ->
         * l[3] = {0}, cvc=1 addElements(0,0,2): levels[0].addValues(2) -> l[0] = {5},cvc=5
         * levels[1].addNulls(2) -> l[1] = {0,3,4}, cvc=5 addElements(1,0,2): levels[0].addValues(2)
         * -> l[0] = {7},cvc=7 levels[1].addElements(2) -> l[1]={0,3,4} cvc = 7
         * levels[2].addNulls(2) -> l[2] = {0,2,3}, cvc=4 addElements(2,0,2): levels[0].addValues(2)
         * -> l[0] = {9},cvc=9 levels[1].addElements(2) -> l[1]={0,3,4} cvc = 9
         * levels[2].addElements(2) -> l[2]={0,2,3} cvc = 6 levels[3].addNulls(2) -> l[3] = {0,1,2},
         * cvc=3
         * 
         * addElements(3,0,2): levels[0].addValues(2) -> l[0] = {11},cvc=11 levels[1].addElements(2)
         * -> l[1]={0,3,4} cvc = 11 levels[2].addElements(2) -> l[2]={0,2,3} cvc = 8
         * levels[3].addElements(2) -> l[3] = {0,1,2}, cvc=5
         * 
         * addElements(2,0,2): levels[0].addValues(1) -> l[0] = {12},cvc=12 levels[1].addElements(1)
         * -> l[1]={0,3,4} cvc = 12 levels[2].addElements(1) -> l[2]={0,2,3} cvc = 9
         * levels[3].addNulls(1) -> l[3] = {0,1,2,5}, cvc = 6
         * 
         * addElements(3,0,2): levels[0].addValues(3) -> l[0] = {15},cvc=15 levels[1].addElements(3)
         * -> l[1]={0,3,4} cvc = 15 levels[2].addElements(3) -> l[2]={0,2,3} cvc = 12
         * levels[3].addElements(3) -> l[3] = {0,1,2,5}, cvc = 9
         * 
         */

        // TODO - reason around alternating 3 repeating and 3 optionals
        for (int i = correspondingDefLevel + 1; i <= defLevel; i++) {
            levelsList[i].addElements(elementsCount);
        }
        if (defLevel + 1 < levelsList.length) {
            levelsList[defLevel + 1].addNulls(elementsCount);
        }
    }


    Pair<Pair<Type.Repetition, IntBuffer>[], Integer> getFinalState() {

        int childCount = levelsList[levelsList.length - 1].childCount();
        if (levelsList.length == 1) {
            childCount--;
        }
        return new Pair<>(
            Arrays.stream(levelsList).skip(1).map(Level::finalState).toArray(Pair[]::new),
            childCount);

    }

    interface Level {

        void addElements(int elementsCount);

        void addValues(int valuesCount);

        void addNulls(int elementsCount);

        Pair<Type.Repetition, IntBuffer> finalState();

        int childCount();
    }

    static class RepeatingLevel implements Level {
        int childValueCount = 0;

        // The last value exclusive for the current range
        private IntBuffer rangeCap = IntBuffer.allocate(4);

        RepeatingLevel() {}

        @Override
        public void addElements(int elementsCount) {
            rangeCap = ensureCapacity(rangeCap, elementsCount);
            for (int i = 0; i < elementsCount; i++) {
                rangeCap.put(++childValueCount);
            }
        }

        @Override
        public void addValues(int valuesCount) {
            rangeCap.put(rangeCap.position() - 1, childValueCount += valuesCount);
        }

        @Override
        public void addNulls(int elementsCount) {
            rangeCap = ensureCapacity(rangeCap, elementsCount);
            for (int i = 0; i < elementsCount; i++) {
                rangeCap.put(childValueCount);
            }
        }

        @Override
        public Pair<Type.Repetition, IntBuffer> finalState() {
            rangeCap.flip();
            return new Pair<>(Type.Repetition.REPEATED, rangeCap);
        }

        @Override
        public int childCount() {
            return childValueCount;
        }

    }

    static class OptionalLevel implements Level {

        int currentCount;
        private IntBuffer nullOffsets = IntBuffer.allocate(4);

        @Override
        public void addElements(int elementsCount) {
            currentCount += elementsCount;
        }

        @Override
        public void addValues(int valuesCount) {
            throw new UnsupportedOperationException(
                "Optional levels don't allow multiple values - use addElements");
        }

        @Override
        public void addNulls(int elementsCount) {
            nullOffsets = ensureCapacity(nullOffsets, elementsCount);
            for (int i = 0; i < elementsCount; i++) {
                nullOffsets.put(currentCount++);
            }
        }

        @Override
        public Pair<Type.Repetition, IntBuffer> finalState() {
            nullOffsets.flip();
            return new Pair<>(Type.Repetition.OPTIONAL, nullOffsets);
        }

        @Override
        public int childCount() {
            return currentCount;
        }

    }

    private static IntBuffer ensureCapacity(IntBuffer nullOffsets, int extraElementsCount) {
        while (nullOffsets.remaining() < extraElementsCount) {
            nullOffsets.flip();
            IntBuffer newBuf = IntBuffer.allocate(nullOffsets.capacity() * 2);
            newBuf.put(nullOffsets);
            nullOffsets = newBuf;
        }
        return nullOffsets;
    }
}

