/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.gui.color.Color;
import io.deephaven.engine.table.Table;
import io.deephaven.util.QueryConstants;

import static io.deephaven.gui.color.Color.*;

public class TestColorUtil extends BaseArrayTestCase {

    private final int size = 10;
    private final Table t1 = TableTools.emptyTable(size).updateView("X = i", "Y = 2*i");

    public void testRowFormatWhereNew() {
        testRowFormatWhere(t1.formatRowWhere("X > 5", "ALICEBLUE"), ALICEBLUE);
        testRowFormatWhere(t1.formatRowWhere("X > 5", "`#F0F8FF`"), ALICEBLUE);
        testRowFormatWhere(t1.formatRowWhere("X > 5", "`aliceblue`"), ALICEBLUE);
        testRowFormatWhere(t1.formatRowWhere("X > 5", "io.deephaven.gui.color.Color.color(`aliceblue`)"), ALICEBLUE);
    }

    public void testRowFormatWhereOld() {
        testRowFormatWhere(t1.formatRowWhere("X > 5", "VIVID_RED"), VIVID_RED);
        testRowFormatWhere(t1.formatRowWhere("X > 5", "`VIVID_RED`"), VIVID_RED);
        testRowFormatWhere(t1.formatRowWhere("X > 5", "io.deephaven.gui.color.Color.color(`VIVID_RED`)"), VIVID_RED);
    }

    public void testFormatColumnsNew() {
        testFormatColumns(t1.formatColumns("X = i > 200 ? NO_FORMATTING : ALICEBLUE"), ALICEBLUE);
        testFormatColumns(t1.formatColumns("X = `#F0F8FF`"), ALICEBLUE);
        testFormatColumns(t1.formatColumns("X = `aliceblue`"), ALICEBLUE);
        testFormatColumns(t1.formatColumns("X = io.deephaven.gui.color.Color.color(`aliceblue`)"), ALICEBLUE);
    }

    public void testFormatColumnsOld() {
        testFormatColumns(t1.formatColumns("X = i > 200 ? NO_FORMATTING : VIVID_RED"), VIVID_RED);
        testFormatColumns(t1.formatColumns("X = `VIVID_RED`"), VIVID_RED);
        testFormatColumns(t1.formatColumns("X = io.deephaven.gui.color.Color.color(`VIVID_RED`)"), VIVID_RED);
    }

    public void testBackground() {
        assertEquals("111111111111111111111111100000000000000000000000000000000",
                Long.toBinaryString(ColorUtil.background(Color.colorRGB(255, 255, 255))));
        assertEquals("100000000000000000000000000000000000000000000000000000000",
                Long.toBinaryString(ColorUtil.background(Color.colorRGB(0, 0, 0))));

        assertEquals(ColorUtil.bg(ALICEBLUE), ColorUtil.background(ALICEBLUE));
        assertEquals(ColorUtil.bg(ALICEBLUE), ColorUtil.background("ALICEBLUE"));
        assertEquals(ColorUtil.bg(ALICEBLUE), ColorUtil.background("#F0F8FF"));


        t1.formatColumns("X = bg(ALICEBLUE)");
        t1.formatColumns("X = bg(`ALICEBLUE`)");
        t1.formatColumns("X = bg(`#FF00FF`)");
        t1.formatColumns("X = bg(VIVID_RED)");
        t1.formatColumns("X = bg(10,20,30)");
        t1.formatColumns("X = background(VIVID_RED)");
    }

    public void testForeground() {
        assertEquals("101111111111111111111111111",
                Long.toBinaryString(ColorUtil.foreground(Color.colorRGB(255, 255, 255))));
        assertEquals("101000000000000000000000000",
                Long.toBinaryString(ColorUtil.foreground(Color.colorRGB(0, 0, 0))));

        assertEquals(ColorUtil.fg(ALICEBLUE), ColorUtil.foreground(Color.colorRGB(240, 248, 255)));
        assertEquals(ColorUtil.fg(ALICEBLUE), ColorUtil.foreground(ALICEBLUE));
        assertEquals(ColorUtil.fg(ALICEBLUE), ColorUtil.foreground("ALICEBLUE"));
        assertEquals(ColorUtil.fg(ALICEBLUE), ColorUtil.foreground("#F0F8FF"));
        assertEquals(ColorUtil.fg(ALICEBLUE), ColorUtil.fg(240, 248, 255));
        assertEquals(ColorUtil.fg(ALICEBLUE), ColorUtil.fg("#F0F8FF"));


        t1.formatColumns("X = fg(ALICEBLUE)");
        t1.formatColumns("X = fg(`ALICEBLUE`)");
        t1.formatColumns("X = fg(`#F0F8FF`)");
        t1.formatColumns("X = fg(VIVID_RED)");
        t1.formatColumns("X = fg(10,20,30)");
        t1.formatColumns("X = foreground(VIVID_RED)");
    }

    public void testForegroundOverride() {
        assertEquals("111111111111111111111111111",
                Long.toBinaryString(ColorUtil.foregroundOverride(Color.colorRGB(255, 255, 255))));
        assertEquals("111000000000000000000000000",
                Long.toBinaryString(ColorUtil.foregroundOverride(Color.colorRGB(0, 0, 0))));

        assertEquals(ColorUtil.fgo(ALICEBLUE), ColorUtil.foregroundOverride(Color.colorRGB(240, 248, 255)));
        assertEquals(ColorUtil.fgo(ALICEBLUE), ColorUtil.foregroundOverride(ALICEBLUE));
        assertEquals(ColorUtil.fgo(ALICEBLUE), ColorUtil.foregroundOverride("ALICEBLUE"));
        assertEquals(ColorUtil.fgo(ALICEBLUE), ColorUtil.foregroundOverride("#F0F8FF"));
        assertEquals(ColorUtil.fgo(ALICEBLUE), ColorUtil.fgo(ColorUtil.bgfga(240, 248, 255)));
        assertEquals(ColorUtil.fgo(ALICEBLUE), ColorUtil.fgo("ALICEBLUE"));


        t1.formatColumns("X = foregroundOverride(ALICEBLUE)");
        t1.formatColumns("X = foregroundOverride(`ALICEBLUE`)");
        t1.formatColumns("X = foregroundOverride(VIVID_RED)");
        t1.formatColumns("X = fgo(VIVID_RED)");
        t1.formatColumns("X = fgo(`#F0F8FF`)");

        assertTrue(ColorUtil.isForegroundSelectionOverride(ColorUtil.fgo(ALICEBLUE)));
        assertFalse(ColorUtil.isForegroundSelectionOverride(ColorUtil.fg(ALICEBLUE)));
        assertFalse(ColorUtil.isForegroundSelectionOverride(ColorUtil.bg(ALICEBLUE)));
        assertFalse(ColorUtil.isForegroundSelectionOverride(ColorUtil.bgo(ALICEBLUE)));
    }

    public void testBackgroundOverride() {
        assertEquals("1111111111111111111111111100000000000000000000000000000000",
                Long.toBinaryString(ColorUtil.backgroundOverride(Color.colorRGB(255, 255, 255))));
        assertEquals("1100000000000000000000000000000000000000000000000000000000",
                Long.toBinaryString(ColorUtil.backgroundOverride(Color.colorRGB(0, 0, 0))));

        assertEquals(ColorUtil.bgo(ALICEBLUE), ColorUtil.backgroundOverride(Color.colorRGB(240, 248, 255)));
        assertEquals(ColorUtil.bgo(ALICEBLUE), ColorUtil.backgroundOverride(ALICEBLUE));
        assertEquals(ColorUtil.bgo(ALICEBLUE), ColorUtil.backgroundOverride("ALICEBLUE"));
        assertEquals(ColorUtil.bgo(ALICEBLUE), ColorUtil.backgroundOverride("#F0F8FF"));
        assertEquals(ColorUtil.bgo(ALICEBLUE), ColorUtil.bgo(ColorUtil.bgfga(240, 248, 255)));
        assertEquals(ColorUtil.bgo(ALICEBLUE), ColorUtil.bgo("ALICEBLUE"));


        t1.formatColumns("X = backgroundOverride(ALICEBLUE)");
        t1.formatColumns("X = backgroundOverride(`ALICEBLUE`)");
        t1.formatColumns("X = backgroundOverride(VIVID_RED)");
        t1.formatColumns("X = bgo(VIVID_RED)");
        t1.formatColumns("X = bgo(`#F0F8FF`)");

        assertTrue(ColorUtil.isBackgroundSelectionOverride(ColorUtil.bgo(ALICEBLUE)));
        assertFalse(ColorUtil.isBackgroundSelectionOverride(ColorUtil.bg(ALICEBLUE)));
        assertFalse(ColorUtil.isBackgroundSelectionOverride(ColorUtil.fg(ALICEBLUE)));
        assertFalse(ColorUtil.isBackgroundSelectionOverride(ColorUtil.fgo(ALICEBLUE)));
    }

    public void testBackgroundForeground() {
        assertEquals("111111111111111111111111100000101111111111111111111111111", Long.toBinaryString(
                ColorUtil.backgroundForeground(Color.colorRGB(255, 255, 255), Color.colorRGB(255, 255, 255))));
        assertEquals("100000000000000000000000000000101000000000000000000000000", Long
                .toBinaryString(ColorUtil.backgroundForeground(Color.colorRGB(0, 0, 0), Color.colorRGB(0, 0, 0))));

        assertEquals(ColorUtil.bgfg(ALICEBLUE, ANTIQUEWHITE),
                ColorUtil.backgroundForeground(Color.colorRGB(240, 248, 255), Color.colorRGB(250, 235, 215)));
        assertEquals(ColorUtil.bgfg(ALICEBLUE, ANTIQUEWHITE),
                ColorUtil.backgroundForeground(ALICEBLUE, ANTIQUEWHITE));
        assertEquals(ColorUtil.bgfg(ALICEBLUE, ANTIQUEWHITE), ColorUtil.backgroundForeground("#F0F8FF", "#FAEBD7"));
        assertEquals(ColorUtil.bgfg(ALICEBLUE, ANTIQUEWHITE), ColorUtil.bgfg("#F0F8FF", "#FAEBD7"));
        assertEquals(ColorUtil.bgfg(ALICEBLUE, ANTIQUEWHITE), ColorUtil.bgfg(240, 248, 255, 250, 235, 215));


        t1.formatColumns("X = bgfg(ALICEBLUE,ANTIQUEWHITE)");
        t1.formatColumns("X = bgfg(VIVID_RED,VIVID_BLUE)");
        t1.formatColumns("X = bgfg(`VIVID_RED`,`VIVID_BLUE`)");
        t1.formatColumns("X = bgfg(240, 248, 255,250,235,215)");
        t1.formatColumns("X = backgroundForeground(ALICEBLUE,ANTIQUEWHITE)");

        assertFalse(ColorUtil.isBackgroundSelectionOverride(ColorUtil.bgfg(ALICEBLUE, ANTIQUEWHITE)));
        assertFalse(ColorUtil.isForegroundSelectionOverride(ColorUtil.bgfg(ALICEBLUE, ANTIQUEWHITE)));
        assertFalse(ColorUtil.isBackgroundSelectionOverride(
                ColorUtil.bgfg(ColorUtil.bg(ALICEBLUE), ColorUtil.fg(ANTIQUEWHITE))));
        assertFalse(ColorUtil.isForegroundSelectionOverride(
                ColorUtil.bgfg(ColorUtil.bg(ALICEBLUE), ColorUtil.fg(ANTIQUEWHITE))));
        assertTrue(ColorUtil.isBackgroundSelectionOverride(
                ColorUtil.bgfg(ColorUtil.bgo(ALICEBLUE), ColorUtil.fg(ANTIQUEWHITE))));
        assertFalse(ColorUtil.isForegroundSelectionOverride(
                ColorUtil.bgfg(ColorUtil.bgo(ALICEBLUE), ColorUtil.fg(ANTIQUEWHITE))));
        assertFalse(ColorUtil.isBackgroundSelectionOverride(
                ColorUtil.bgfg(ColorUtil.bg(ALICEBLUE), ColorUtil.fgo(ANTIQUEWHITE))));
        assertTrue(ColorUtil.isForegroundSelectionOverride(
                ColorUtil.bgfg(ColorUtil.bg(ALICEBLUE), ColorUtil.fgo(ANTIQUEWHITE))));
        assertTrue(ColorUtil.isBackgroundSelectionOverride(
                ColorUtil.bgfg(ColorUtil.bgo(ALICEBLUE), ColorUtil.fgo(ANTIQUEWHITE))));
        assertTrue(ColorUtil.isForegroundSelectionOverride(
                ColorUtil.bgfg(ColorUtil.bgo(ALICEBLUE), ColorUtil.fgo(ANTIQUEWHITE))));
    }

    public void testBackgroundForegroundAuto() {
        assertEquals("111111111111111111111111100000001000000000000000000000000",
                Long.toBinaryString(ColorUtil.bgfga(255, 255, 255)));
        assertEquals("100000000000000000000000000000001111000001110000011100000",
                Long.toBinaryString(ColorUtil.bgfga(0, 0, 0)));

        assertEquals(ColorUtil.bgfga(ALICEBLUE), ColorUtil.backgroundForegroundAuto(Color.colorRGB(240, 248, 255)));
        assertEquals(ColorUtil.bgfga(ALICEBLUE), ColorUtil.backgroundForegroundAuto(ALICEBLUE));
        assertEquals(ColorUtil.bgfga(ALICEBLUE), ColorUtil.backgroundForegroundAuto("ALICEBLUE"));
        assertEquals(ColorUtil.bgfga(ALICEBLUE), ColorUtil.backgroundForegroundAuto("#F0F8FF"));
        assertEquals(ColorUtil.bgfga(ALICEBLUE), ColorUtil.bgfga("#F0F8FF"));
        assertEquals(ColorUtil.bgfga(ALICEBLUE), ColorUtil.bgfga(240, 248, 255));


        t1.formatColumns("X = bgfga(ALICEBLUE)");
        t1.formatColumns("X = bgfga(VIVID_RED)");
        t1.formatColumns("X = bgfga(`VIVID_RED`)");
        t1.formatColumns("X = bgfga(`#F0F8FF`)");
        t1.formatColumns("X = bgfga(240, 248, 255)");
        t1.formatColumns("X = backgroundForegroundAuto(ALICEBLUE)");
    }

    public void testHeatmap() {
        assertEquals(ColorUtil.bgfga(RED), ColorUtil.heatmap(0, 0, 100, RED, BLUE));
        assertEquals(ColorUtil.bgfga(RED),
                ColorUtil.heatmap(0, 0, 100, Color.colorRGB(255, 0, 0), Color.colorRGB(0, 0, 255)));
        assertEquals(ColorUtil.bgfga(191, 0, 63), ColorUtil.heatmap(25, 0, 100, RED, BLUE));
        assertEquals(ColorUtil.bgfga(127, 0, 127), ColorUtil.heatmap(50, 0, 100, RED, BLUE));
        assertEquals(ColorUtil.bgfga(63, 0, 191), ColorUtil.heatmap(75, 0, 100, RED, BLUE));
        assertEquals(ColorUtil.bgfga(63, 0, 191), ColorUtil.heatmap(75, 0, 100, "RED", "BLUE"));


        t1.formatColumns("X = heatmap(100, 0, 100, RED, BLUE)");
        t1.formatColumns("X = heatmap(100, 0, 100, `RED`, `BLUE`)");
        t1.formatColumns("X = heatmap(2, 0, 100, bgfga(RED), bgfga(BLUE))");
        t1.formatColumns("X = heatmap(2, 0, 100, bgfga(`#F0F8FF`), bgfga(`#F0F80F`))");
    }

    public void testHeatmapForeground() {
        assertEquals(ColorUtil.fg(RED), ColorUtil.heatmapForeground(0, 0, 100, RED, BLUE));
        assertEquals(ColorUtil.fg(BLUE), ColorUtil.heatmapForeground(100, 0, 100, RED, BLUE));
        assertEquals(ColorUtil.fg(127, 0, 127), ColorUtil.heatmapForeground(50, 0, 100, RED, BLUE));
        assertEquals(ColorUtil.fg(127, 0, 127), ColorUtil.heatmapForeground(50, 0, 100, "RED", "BLUE"));

        assertEquals(ColorUtil.heatmapFg(0, 0, 100, ColorUtil.fg(RED), ColorUtil.fg(BLUE)),
                ColorUtil.heatmapForeground(0, 0, 100, RED, BLUE));
        assertEquals(ColorUtil.heatmapFg(50, 0, 100, ColorUtil.fg(RED), ColorUtil.fg(BLUE)),
                ColorUtil.heatmapForeground(50, 0, 100, RED, BLUE));
        assertEquals(ColorUtil.heatmapFg(50, 0, 100, ColorUtil.fg(RED), ColorUtil.fg(BLUE)),
                ColorUtil.heatmapFg(50, 0, 100, "RED", "BLUE"));
        assertEquals(ColorUtil.heatmapFg(50, 0, 100, ColorUtil.fg(RED), ColorUtil.fg(BLUE)),
                ColorUtil.heatmapFg(50, 0, 100, RED, BLUE));
        assertEquals(ColorUtil.heatmapFg(50, 0, 100, ColorUtil.fg(RED), ColorUtil.fg(BLUE)),
                ColorUtil.heatmapFg(50, 0, 100, "#FF0000", "BLUE"));

        t1.formatColumns("X = heatmapFg(100, 0, 100, RED, BLUE)");
        t1.formatColumns("X = heatmapFg(100, 0, 100, `RED`, `BLUE`)");
        t1.formatColumns("X = heatmapFg(100, 0, 100, fg(RED), fg(BLUE))");
        t1.formatColumns("X = heatmapFg(100, 0, 100, fg(`RED`), fg(BLUE))");
        t1.formatColumns("X = heatmapForeground(100, 0, 100, RED, BLUE)");
    }

    public void testToLong() {
        assertEquals(ColorUtil.bgfga(ALICEBLUE), ColorUtil.toLong(ALICEBLUE));
        assertEquals(0L, ColorUtil.toLong((Color) null));
        assertEquals(0L, ColorUtil.toLong(QueryConstants.NULL_LONG));

        assertEquals(ColorUtil.bgfga(ALICEBLUE), ColorUtil.toLong("ALICEBLUE"));
        assertEquals(0L, ColorUtil.toLong((String) null));

        assertEquals(ColorUtil.bgfga(ALICEBLUE), ColorUtil.toLong("#F0F8FF"));
        assertEquals(0L, ColorUtil.toLong((String) null));

        assertEquals(ColorUtil.bgfga(ALICEBLUE), ColorUtil.toLong(Color.color("ALICEBLUE")));
        assertEquals(0L, ColorUtil.toLong((Color) null));

        assertEquals(ColorUtil.bgfga(ALICEBLUE), ColorUtil.toLong(Color.colorRGB(240, 248, 255)));
        assertEquals(0L, ColorUtil.toLong((Color) null));

        assertEquals(ColorUtil.bgfga(0, 0, 0), ColorUtil.toLong(Color.colorRGB(0, 0, 0)));
        assertEquals(0L, ColorUtil.toLong(Color.NO_FORMATTING));
    }

    public void testIsForegroundSet() {
        assertTrue(ColorUtil.isForegroundSet(ColorUtil.fg(240, 248, 255)));
        assertTrue(ColorUtil.isForegroundSet(ColorUtil.fg(ALICEBLUE)));
        assertTrue(ColorUtil.isForegroundSet(ColorUtil.fg("ALICEBLUE")));
        assertTrue(ColorUtil.isForegroundSet(ColorUtil.fg(ColorUtil.toLong("ALICEBLUE"))));

        assertFalse(ColorUtil.isForegroundSet(ColorUtil.bg(240, 248, 255)));
        assertFalse(ColorUtil.isForegroundSet(ColorUtil.bg(ALICEBLUE)));
        assertFalse(ColorUtil.isForegroundSet(ColorUtil.bg("ALICEBLUE")));
        assertFalse(ColorUtil.isForegroundSet(ColorUtil.bg(ColorUtil.toLong("ALICEBLUE"))));
        assertFalse(ColorUtil.isForegroundSet(ColorUtil.toLong(ALICEBLUE)));
        assertFalse(ColorUtil.isForegroundSet(ColorUtil.toLong("ALICEBLUE")));
    }

    private void testRowFormatWhere(final Table colorTable, final Color color) {
        final long[] colorTableCol =
                colorTable.getColumn(ColumnFormattingValues.ROW_FORMAT_NAME + ColumnFormattingValues.TABLE_FORMAT_NAME)
                        .getLongs(0, size);

        for (int i = 0; i < 6; i++) {
            // assertEquals(0L, colorTableCol[i]);
        }
        for (int i = 6; i < size; i++) {
            assertEquals(ColorUtil.toLong(color), colorTableCol[i]);
        }
    }

    private void testFormatColumns(final Table colorTable, final Color color) {
        final long[] colorTableCol =
                colorTable.getColumn("X" + ColumnFormattingValues.TABLE_FORMAT_NAME).getLongs(0, size);
        for (long aColorTableCol : colorTableCol) {
            assertEquals(ColorUtil.toLong(color), aColorTableCol);
        }
    }
}
