package io.deephaven.engine.table.impl;

import io.deephaven.engine.util.ColumnRenderersBuilder;
import junit.framework.TestCase;
import org.junit.Test;

public class TestColumnRenderersBuilder extends TestCase {

    public static final String TEST_DIRECTIVE_ENUM = "A=DEFAULT,B=PROGRESS_BAR,C=PROGRESS_BAR,";
    public static final String TEST_DIRECTIVE_CLASS =
            "A=io.deephaven.gui.table.ColumnRenderer,B=io.deephaven.gui.table.TextAreaColumnRenderer,C=io.deephaven.console.events.ProgressRenderer,";
    public static final String TEST_DIRECTIVE_MIXED =
            "A=DEFAULT,B=io.deephaven.gui.table.TextAreaColumnRenderer,C=PROGRESS_BAR,";

    @Test
    public void testBuildDirectiveEnum() {
        ColumnRenderersBuilder builder = new ColumnRenderersBuilder();
        builder.setRenderer("A", ColumnRenderersBuilder.ColumnRendererType.DEFAULT);
        builder.setRenderer("B", ColumnRenderersBuilder.ColumnRendererType.PROGRESS_BAR);
        builder.setRenderer("C", ColumnRenderersBuilder.ColumnRendererType.PROGRESS_BAR);

        assertEquals(TEST_DIRECTIVE_ENUM, builder.buildDirective());
    }

    @Test
    public void testFromDirectiveEnum() {
        ColumnRenderersBuilder builder = ColumnRenderersBuilder.fromDirective(TEST_DIRECTIVE_ENUM);

        assertTrue(builder.isColumnRendererSet("A"));
        assertTrue(builder.isColumnRendererSet("B"));
        assertTrue(builder.isColumnRendererSet("C"));
        assertFalse(builder.isColumnRendererSet("D"));

        assertEquals(ColumnRenderersBuilder.ColumnRendererType.DEFAULT, builder.getRendererType("A"));
        assertEquals(ColumnRenderersBuilder.ColumnRendererType.PROGRESS_BAR, builder.getRendererType("B"));
        assertEquals(ColumnRenderersBuilder.ColumnRendererType.PROGRESS_BAR, builder.getRendererType("C"));
        assertNull(builder.getRendererType("D"));

        assertEquals(builder.getRenderClassForType(ColumnRenderersBuilder.ColumnRendererType.DEFAULT),
                builder.getRenderClassName("A"));
        assertEquals(builder.getRenderClassForType(ColumnRenderersBuilder.ColumnRendererType.PROGRESS_BAR),
                builder.getRenderClassName("B"));
        assertEquals(builder.getRenderClassForType(ColumnRenderersBuilder.ColumnRendererType.PROGRESS_BAR),
                builder.getRenderClassName("C"));
        assertNull(builder.getRenderClassName("D"));
    }

    @Test
    public void testBuildDirectiveClass() {
        ColumnRenderersBuilder builder = new ColumnRenderersBuilder();
        builder.setRenderer("A", "io.deephaven.gui.table.ColumnRenderer");
        builder.setRenderer("B", "io.deephaven.gui.table.TextAreaColumnRenderer");
        builder.setRenderer("C", "io.deephaven.console.events.ProgressRenderer");

        assertEquals(TEST_DIRECTIVE_CLASS, builder.buildDirective());
    }

    @Test
    public void testFromDirectiveClass() {
        ColumnRenderersBuilder builder = ColumnRenderersBuilder.fromDirective(TEST_DIRECTIVE_CLASS);

        assertTrue(builder.isColumnRendererSet("A"));
        assertTrue(builder.isColumnRendererSet("B"));
        assertTrue(builder.isColumnRendererSet("C"));
        assertFalse(builder.isColumnRendererSet("D"));

        assertNull(builder.getRendererType("A"));
        assertNull(builder.getRendererType("B"));
        assertNull(builder.getRendererType("C"));
        assertNull(builder.getRendererType("D"));

        assertEquals("io.deephaven.gui.table.ColumnRenderer", builder.getRenderClassName("A"));
        assertEquals("io.deephaven.gui.table.TextAreaColumnRenderer", builder.getRenderClassName("B"));
        assertEquals("io.deephaven.console.events.ProgressRenderer", builder.getRenderClassName("C"));
        assertNull(builder.getRenderClassName("D"));
    }

    @Test
    public void testBuildDirectiveMixed() {
        ColumnRenderersBuilder builder = new ColumnRenderersBuilder();
        builder.setRenderer("A", ColumnRenderersBuilder.ColumnRendererType.DEFAULT);
        builder.setRenderer("B", "io.deephaven.gui.table.TextAreaColumnRenderer");
        builder.setRenderer("C", ColumnRenderersBuilder.ColumnRendererType.PROGRESS_BAR);

        assertEquals(TEST_DIRECTIVE_MIXED, builder.buildDirective());
    }

    @Test
    public void testFromDirectiveMixed() {
        ColumnRenderersBuilder builder = ColumnRenderersBuilder.fromDirective(TEST_DIRECTIVE_MIXED);

        assertTrue(builder.isColumnRendererSet("A"));
        assertTrue(builder.isColumnRendererSet("B"));
        assertTrue(builder.isColumnRendererSet("C"));
        assertFalse(builder.isColumnRendererSet("D"));

        assertEquals(ColumnRenderersBuilder.ColumnRendererType.DEFAULT, builder.getRendererType("A"));
        assertNull(builder.getRendererType("B"));
        assertEquals(ColumnRenderersBuilder.ColumnRendererType.PROGRESS_BAR, builder.getRendererType("C"));
        assertNull(builder.getRendererType("D"));

        assertEquals(builder.getRenderClassForType(ColumnRenderersBuilder.ColumnRendererType.DEFAULT),
                builder.getRenderClassName("A"));
        assertEquals("io.deephaven.gui.table.TextAreaColumnRenderer", builder.getRenderClassName("B"));
        assertEquals(builder.getRenderClassForType(ColumnRenderersBuilder.ColumnRendererType.PROGRESS_BAR),
                builder.getRenderClassName("C"));
        assertNull(builder.getRenderClassName("D"));
    }

    @Test
    public void testSetRenderClass() {
        ColumnRenderersBuilder builder = new ColumnRenderersBuilder();

        builder.setRenderClass(ColumnRenderersBuilder.ColumnRendererType.DEFAULT, "a.b.c");
        builder.setRenderClass(ColumnRenderersBuilder.ColumnRendererType.PROGRESS_BAR, "p.d.q");

        builder.setRenderer("A", ColumnRenderersBuilder.ColumnRendererType.DEFAULT);
        builder.setRenderer("B", ColumnRenderersBuilder.ColumnRendererType.PROGRESS_BAR);

        assertEquals("a.b.c", builder.getRenderClassName("A"));
        assertEquals("p.d.q", builder.getRenderClassName("B"));
    }

    @Test
    public void testSetDefault() {
        ColumnRenderersBuilder builder = new ColumnRenderersBuilder();

        builder.setDefaultRenderClass("a.b.c");
        builder.setRenderer("A", ColumnRenderersBuilder.ColumnRendererType.DEFAULT);

        assertEquals("a.b.c", builder.getRenderClassName("A"));
    }
}
