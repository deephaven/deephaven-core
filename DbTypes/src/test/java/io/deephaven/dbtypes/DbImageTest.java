package io.deephaven.dbtypes;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.configuration.Configuration;

import javax.imageio.ImageIO;
import java.io.File;
import java.io.IOException;

public class DbImageTest extends BaseArrayTestCase {
    final private String fileName = Configuration.getInstance().getDevRootPath()
        + "/DbTypesImpl/src/test/resources/io/deephaven/dbtypes/white.jpg";

    public void testNewInstanceByteArray() throws IOException {
        final DbImage f = DbImage.newInstance(ImageIO.read(new java.io.File(fileName)));
        assertNotNull(f);
    }

    public void testNewInstanceString() {
        final DbImage f = DbImage.newInstance(fileName);
        assertNotNull(f);
    }

    public void testNewInstanceFile() {
        final File file = new File(fileName);
        final DbImage f = DbImage.newInstance(file);
        assertNotNull(f);
    }

    public void testNewInstanceBufferedImage() throws IOException {
        final DbImage f = DbImage.newInstance(ImageIO.read(new java.io.File(fileName)));
        assertNotNull(f);
    }
}
