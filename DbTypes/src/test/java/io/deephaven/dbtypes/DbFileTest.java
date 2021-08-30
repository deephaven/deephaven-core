package io.deephaven.dbtypes;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.configuration.Configuration;

import java.io.File;

public class DbFileTest extends BaseArrayTestCase {
    final private String fileName = Configuration.getInstance().getDevRootPath() + "/build.gradle";

    public void testNewInstance() {
        final DbFile f = DbFile.newInstance();
        assertNotNull(f);
    }

    public void testNewInstanceByteArray() {
        final DbFile f = DbFile.newInstance(new byte[] {1, 2, 3});
        assertNotNull(f);
    }

    public void testNewInstanceString() {
        final DbFile f = DbFile.newInstance(fileName);
        assertNotNull(f);
    }

    public void testNewInstanceFile() {
        final File file = new File(fileName);
        final DbFile f = DbFile.newInstance(file);
        assertNotNull(f);
    }
}
