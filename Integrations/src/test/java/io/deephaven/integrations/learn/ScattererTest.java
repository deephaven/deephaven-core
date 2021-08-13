package io.deephaven.integrations.learn;

import org.junit.Test;

import java.util.function.Function;

public class ScattererTest {

    @Test
    public void createScattererTest() {
        final Function<Object[], Object> func = args -> args;
        Output[] outputs = new Output[]{new Output("OutCol1", func, "int"),
                                        new Output("OutCol2", func, "boolean"),
                                        new Output("OutCol3", func, "long")};

        Scatterer scatterer = new Scatterer(outputs);
    }

}
