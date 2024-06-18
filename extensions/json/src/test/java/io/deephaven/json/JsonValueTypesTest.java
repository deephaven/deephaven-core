//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonValueTypesTest {

    @Test
    void checkAllowedTypeInvariants() throws InvocationTargetException, IllegalAccessException {
        int count = 0;
        for (Method declaredMethod : JsonValueTypes.class.getDeclaredMethods()) {
            if (declaredMethod.getReturnType().equals(Set.class)) {
                final Set<JsonValueTypes> set = (Set<JsonValueTypes>) declaredMethod.invoke(null);
                JsonValueTypes.checkAllowedTypeInvariants(set);
                ++count;
            }
        }
        assertThat(count).isEqualTo(17);
    }
}
