package io.deephaven.db.util.jpy;

import io.deephaven.jpy.JpyConfig.Flag;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;
import org.jpy.PyLib.Diag;
import org.junit.Assert;
import org.junit.Test;

public class JpyConfigFlagTest {

    private static final Set<String> KNOWN;
    static {
        KNOWN = new HashSet<>();
        KNOWN.add("F_OFF");
        KNOWN.add("F_TYPE");
        KNOWN.add("F_METH");
        KNOWN.add("F_EXEC");
        KNOWN.add("F_MEM");
        KNOWN.add("F_JVM");
        KNOWN.add("F_ERR");
        KNOWN.add("F_ALL");
    }

    @Test
    public void flags() {
        Assert.assertEquals(Flag.OFF.getBitset(), Diag.F_OFF);
        Assert.assertEquals(Flag.TYPE.getBitset(), Diag.F_TYPE);
        Assert.assertEquals(Flag.METH.getBitset(), Diag.F_METH);
        Assert.assertEquals(Flag.EXEC.getBitset(), Diag.F_EXEC);
        Assert.assertEquals(Flag.MEM.getBitset(), Diag.F_MEM);
        Assert.assertEquals(Flag.JVM.getBitset(), Diag.F_JVM);
        Assert.assertEquals(Flag.ERR.getBitset(), Diag.F_ERR);
        Assert.assertEquals(Flag.ALL.getBitset(), Diag.F_ALL);
    }

    @Test
    public void flagCoverage() {
        for (Field field : Diag.class.getDeclaredFields()) {
            if (Modifier.isPublic(field.getModifiers())
                    && Modifier.isStatic(field.getModifiers())
                    && Modifier.isFinal(field.getModifiers())
                    && field.getType().equals(int.class)) {
                Assert.assertTrue(KNOWN.contains(field.getName()));
            }
        }
    }
}
