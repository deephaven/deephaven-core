/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.util;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.gui.color.Color;
import io.deephaven.db.tables.Table;
import io.deephaven.util.QueryConstants;
import io.deephaven.db.tables.utils.TableTools;

import static io.deephaven.gui.color.Color.*;

public class TestDBColorUtil extends BaseArrayTestCase {

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
                Long.toBinaryString(DBColorUtil.background(Color.colorRGB(255, 255, 255))));
        assertEquals("100000000000000000000000000000000000000000000000000000000",
                Long.toBinaryString(DBColorUtil.background(Color.colorRGB(0, 0, 0))));

        assertEquals(DBColorUtil.bg(ALICEBLUE), DBColorUtil.background(ALICEBLUE));
        assertEquals(DBColorUtil.bg(ALICEBLUE), DBColorUtil.background("ALICEBLUE"));
        assertEquals(DBColorUtil.bg(ALICEBLUE), DBColorUtil.background("#F0F8FF"));


        t1.formatColumns("X = bg(ALICEBLUE)");
        t1.formatColumns("X = bg(`ALICEBLUE`)");
        t1.formatColumns("X = bg(`#FF00FF`)");
        t1.formatColumns("X = bg(VIVID_RED)");
        t1.formatColumns("X = bg(10,20,30)");
        t1.formatColumns("X = background(VIVID_RED)");
    }

    public void testForeground() {
        assertEquals("101111111111111111111111111",
                Long.toBinaryString(DBColorUtil.foreground(Color.colorRGB(255, 255, 255))));
        assertEquals("101000000000000000000000000",
                Long.toBinaryString(DBColorUtil.foreground(Color.colorRGB(0, 0, 0))));

        assertEquals(DBColorUtil.fg(ALICEBLUE), DBColorUtil.foreground(Color.colorRGB(240, 248, 255)));
        assertEquals(DBColorUtil.fg(ALICEBLUE), DBColorUtil.foreground(ALICEBLUE));
        assertEquals(DBColorUtil.fg(ALICEBLUE), DBColorUtil.foreground("ALICEBLUE"));
        assertEquals(DBColorUtil.fg(ALICEBLUE), DBColorUtil.foreground("#F0F8FF"));
        assertEquals(DBColorUtil.fg(ALICEBLUE), DBColorUtil.fg(240, 248, 255));
        assertEquals(DBColorUtil.fg(ALICEBLUE), DBColorUtil.fg("#F0F8FF"));


        t1.formatColumns("X = fg(ALICEBLUE)");
        t1.formatColumns("X = fg(`ALICEBLUE`)");
        t1.formatColumns("X = fg(`#F0F8FF`)");
        t1.formatColumns("X = fg(VIVID_RED)");
        t1.formatColumns("X = fg(10,20,30)");
        t1.formatColumns("X = foreground(VIVID_RED)");
    }

    public void testForegroundOverride() {
        assertEquals("111111111111111111111111111",
                Long.toBinaryString(DBColorUtil.foregroundOverride(Color.colorRGB(255, 255, 255))));
        assertEquals("111000000000000000000000000",
                Long.toBinaryString(DBColorUtil.foregroundOverride(Color.colorRGB(0, 0, 0))));

        assertEquals(DBColorUtil.fgo(ALICEBLUE), DBColorUtil.foregroundOverride(Color.colorRGB(240, 248, 255)));
        assertEquals(DBColorUtil.fgo(ALICEBLUE), DBColorUtil.foregroundOverride(ALICEBLUE));
        assertEquals(DBColorUtil.fgo(ALICEBLUE), DBColorUtil.foregroundOverride("ALICEBLUE"));
        assertEquals(DBColorUtil.fgo(ALICEBLUE), DBColorUtil.foregroundOverride("#F0F8FF"));
        assertEquals(DBColorUtil.fgo(ALICEBLUE), DBColorUtil.fgo(DBColorUtil.bgfga(240, 248, 255)));
        assertEquals(DBColorUtil.fgo(ALICEBLUE), DBColorUtil.fgo("ALICEBLUE"));


        t1.formatColumns("X = foregroundOverride(ALICEBLUE)");
        t1.formatColumns("X = foregroundOverride(`ALICEBLUE`)");
        t1.formatColumns("X = foregroundOverride(VIVID_RED)");
        t1.formatColumns("X = fgo(VIVID_RED)");
        t1.formatColumns("X = fgo(`#F0F8FF`)");

        assertTrue(DBColorUtil.isForegroundSelectionOverride(DBColorUtil.fgo(ALICEBLUE)));
        assertFalse(DBColorUtil.isForegroundSelectionOverride(DBColorUtil.fg(ALICEBLUE)));
        assertFalse(DBColorUtil.isForegroundSelectionOverride(DBColorUtil.bg(ALICEBLUE)));
        assertFalse(DBColorUtil.isForegroundSelectionOverride(DBColorUtil.bgo(ALICEBLUE)));
    }

    public void testBackgroundOverride() {
        assertEquals("1111111111111111111111111100000000000000000000000000000000",
                Long.toBinaryString(DBColorUtil.backgroundOverride(Color.colorRGB(255, 255, 255))));
        assertEquals("1100000000000000000000000000000000000000000000000000000000",
                Long.toBinaryString(DBColorUtil.backgroundOverride(Color.colorRGB(0, 0, 0))));

        assertEquals(DBColorUtil.bgo(ALICEBLUE), DBColorUtil.backgroundOverride(Color.colorRGB(240, 248, 255)));
        assertEquals(DBColorUtil.bgo(ALICEBLUE), DBColorUtil.backgroundOverride(ALICEBLUE));
        assertEquals(DBColorUtil.bgo(ALICEBLUE), DBColorUtil.backgroundOverride("ALICEBLUE"));
        assertEquals(DBColorUtil.bgo(ALICEBLUE), DBColorUtil.backgroundOverride("#F0F8FF"));
        assertEquals(DBColorUtil.bgo(ALICEBLUE), DBColorUtil.bgo(DBColorUtil.bgfga(240, 248, 255)));
        assertEquals(DBColorUtil.bgo(ALICEBLUE), DBColorUtil.bgo("ALICEBLUE"));


        t1.formatColumns("X = backgroundOverride(ALICEBLUE)");
        t1.formatColumns("X = backgroundOverride(`ALICEBLUE`)");
        t1.formatColumns("X = backgroundOverride(VIVID_RED)");
        t1.formatColumns("X = bgo(VIVID_RED)");
        t1.formatColumns("X = bgo(`#F0F8FF`)");

        assertTrue(DBColorUtil.isBackgroundSelectionOverride(DBColorUtil.bgo(ALICEBLUE)));
        assertFalse(DBColorUtil.isBackgroundSelectionOverride(DBColorUtil.bg(ALICEBLUE)));
        assertFalse(DBColorUtil.isBackgroundSelectionOverride(DBColorUtil.fg(ALICEBLUE)));
        assertFalse(DBColorUtil.isBackgroundSelectionOverride(DBColorUtil.fgo(ALICEBLUE)));
    }

    public void testBackgroundForeground() {
        assertEquals("111111111111111111111111100000101111111111111111111111111", Long.toBinaryString(
                DBColorUtil.backgroundForeground(Color.colorRGB(255, 255, 255), Color.colorRGB(255, 255, 255))));
        assertEquals("100000000000000000000000000000101000000000000000000000000", Long
                .toBinaryString(DBColorUtil.backgroundForeground(Color.colorRGB(0, 0, 0), Color.colorRGB(0, 0, 0))));

        assertEquals(DBColorUtil.bgfg(ALICEBLUE, ANTIQUEWHITE),
                DBColorUtil.backgroundForeground(Color.colorRGB(240, 248, 255), Color.colorRGB(250, 235, 215)));
        assertEquals(DBColorUtil.bgfg(ALICEBLUE, ANTIQUEWHITE),
                DBColorUtil.backgroundForeground(ALICEBLUE, ANTIQUEWHITE));
        assertEquals(DBColorUtil.bgfg(ALICEBLUE, ANTIQUEWHITE), DBColorUtil.backgroundForeground("#F0F8FF", "#FAEBD7"));
        assertEquals(DBColorUtil.bgfg(ALICEBLUE, ANTIQUEWHITE), DBColorUtil.bgfg("#F0F8FF", "#FAEBD7"));
        assertEquals(DBColorUtil.bgfg(ALICEBLUE, ANTIQUEWHITE), DBColorUtil.bgfg(240, 248, 255, 250, 235, 215));


        t1.formatColumns("X = bgfg(ALICEBLUE,ANTIQUEWHITE)");
        t1.formatColumns("X = bgfg(VIVID_RED,VIVID_BLUE)");
        t1.formatColumns("X = bgfg(`VIVID_RED`,`VIVID_BLUE`)");
        t1.formatColumns("X = bgfg(240, 248, 255,250,235,215)");
        t1.formatColumns("X = backgroundForeground(ALICEBLUE,ANTIQUEWHITE)");

        assertFalse(DBColorUtil.isBackgroundSelectionOverride(DBColorUtil.bgfg(ALICEBLUE, ANTIQUEWHITE)));
        assertFalse(DBColorUtil.isForegroundSelectionOverride(DBColorUtil.bgfg(ALICEBLUE, ANTIQUEWHITE)));
        assertFalse(DBColorUtil.isBackgroundSelectionOverride(
                DBColorUtil.bgfg(DBColorUtil.bg(ALICEBLUE), DBColorUtil.fg(ANTIQUEWHITE))));
        assertFalse(DBColorUtil.isForegroundSelectionOverride(
                DBColorUtil.bgfg(DBColorUtil.bg(ALICEBLUE), DBColorUtil.fg(ANTIQUEWHITE))));
        assertTrue(DBColorUtil.isBackgroundSelectionOverride(
                DBColorUtil.bgfg(DBColorUtil.bgo(ALICEBLUE), DBColorUtil.fg(ANTIQUEWHITE))));
        assertFalse(DBColorUtil.isForegroundSelectionOverride(
                DBColorUtil.bgfg(DBColorUtil.bgo(ALICEBLUE), DBColorUtil.fg(ANTIQUEWHITE))));
        assertFalse(DBColorUtil.isBackgroundSelectionOverride(
                DBColorUtil.bgfg(DBColorUtil.bg(ALICEBLUE), DBColorUtil.fgo(ANTIQUEWHITE))));
        assertTrue(DBColorUtil.isForegroundSelectionOverride(
                DBColorUtil.bgfg(DBColorUtil.bg(ALICEBLUE), DBColorUtil.fgo(ANTIQUEWHITE))));
        assertTrue(DBColorUtil.isBackgroundSelectionOverride(
                DBColorUtil.bgfg(DBColorUtil.bgo(ALICEBLUE), DBColorUtil.fgo(ANTIQUEWHITE))));
        assertTrue(DBColorUtil.isForegroundSelectionOverride(
                DBColorUtil.bgfg(DBColorUtil.bgo(ALICEBLUE), DBColorUtil.fgo(ANTIQUEWHITE))));
    }

    public void testBackgroundForegroundAuto() {
        assertEquals("111111111111111111111111100000001000000000000000000000000",
                Long.toBinaryString(DBColorUtil.bgfga(255, 255, 255)));
        assertEquals("100000000000000000000000000000001111000001110000011100000",
                Long.toBinaryString(DBColorUtil.bgfga(0, 0, 0)));

        assertEquals(DBColorUtil.bgfga(ALICEBLUE), DBColorUtil.backgroundForegroundAuto(Color.colorRGB(240, 248, 255)));
        assertEquals(DBColorUtil.bgfga(ALICEBLUE), DBColorUtil.backgroundForegroundAuto(ALICEBLUE));
        assertEquals(DBColorUtil.bgfga(ALICEBLUE), DBColorUtil.backgroundForegroundAuto("ALICEBLUE"));
        assertEquals(DBColorUtil.bgfga(ALICEBLUE), DBColorUtil.backgroundForegroundAuto("#F0F8FF"));
        assertEquals(DBColorUtil.bgfga(ALICEBLUE), DBColorUtil.bgfga("#F0F8FF"));
        assertEquals(DBColorUtil.bgfga(ALICEBLUE), DBColorUtil.bgfga(240, 248, 255));


        t1.formatColumns("X = bgfga(ALICEBLUE)");
        t1.formatColumns("X = bgfga(VIVID_RED)");
        t1.formatColumns("X = bgfga(`VIVID_RED`)");
        t1.formatColumns("X = bgfga(`#F0F8FF`)");
        t1.formatColumns("X = bgfga(240, 248, 255)");
        t1.formatColumns("X = backgroundForegroundAuto(ALICEBLUE)");
    }

    public void testHeatmap() {
        assertEquals(DBColorUtil.bgfga(RED), DBColorUtil.heatmap(0, 0, 100, RED, BLUE));
        assertEquals(DBColorUtil.bgfga(RED),
                DBColorUtil.heatmap(0, 0, 100, Color.colorRGB(255, 0, 0), Color.colorRGB(0, 0, 255)));
        assertEquals(DBColorUtil.bgfga(191, 0, 63), DBColorUtil.heatmap(25, 0, 100, RED, BLUE));
        assertEquals(DBColorUtil.bgfga(127, 0, 127), DBColorUtil.heatmap(50, 0, 100, RED, BLUE));
        assertEquals(DBColorUtil.bgfga(63, 0, 191), DBColorUtil.heatmap(75, 0, 100, RED, BLUE));
        assertEquals(DBColorUtil.bgfga(63, 0, 191), DBColorUtil.heatmap(75, 0, 100, "RED", "BLUE"));


        t1.formatColumns("X = heatmap(100, 0, 100, RED, BLUE)");
        t1.formatColumns("X = heatmap(100, 0, 100, `RED`, `BLUE`)");
        t1.formatColumns("X = heatmap(2, 0, 100, bgfga(RED), bgfga(BLUE))");
        t1.formatColumns("X = heatmap(2, 0, 100, bgfga(`#F0F8FF`), bgfga(`#F0F80F`))");
    }

    public void testHeatmapForeground() {
        assertEquals(DBColorUtil.fg(RED), DBColorUtil.heatmapForeground(0, 0, 100, RED, BLUE));
        assertEquals(DBColorUtil.fg(BLUE), DBColorUtil.heatmapForeground(100, 0, 100, RED, BLUE));
        assertEquals(DBColorUtil.fg(127, 0, 127), DBColorUtil.heatmapForeground(50, 0, 100, RED, BLUE));
        assertEquals(DBColorUtil.fg(127, 0, 127), DBColorUtil.heatmapForeground(50, 0, 100, "RED", "BLUE"));

        assertEquals(DBColorUtil.heatmapFg(0, 0, 100, DBColorUtil.fg(RED), DBColorUtil.fg(BLUE)),
                DBColorUtil.heatmapForeground(0, 0, 100, RED, BLUE));
        assertEquals(DBColorUtil.heatmapFg(50, 0, 100, DBColorUtil.fg(RED), DBColorUtil.fg(BLUE)),
                DBColorUtil.heatmapForeground(50, 0, 100, RED, BLUE));
        assertEquals(DBColorUtil.heatmapFg(50, 0, 100, DBColorUtil.fg(RED), DBColorUtil.fg(BLUE)),
                DBColorUtil.heatmapFg(50, 0, 100, "RED", "BLUE"));
        assertEquals(DBColorUtil.heatmapFg(50, 0, 100, DBColorUtil.fg(RED), DBColorUtil.fg(BLUE)),
                DBColorUtil.heatmapFg(50, 0, 100, RED, BLUE));
        assertEquals(DBColorUtil.heatmapFg(50, 0, 100, DBColorUtil.fg(RED), DBColorUtil.fg(BLUE)),
                DBColorUtil.heatmapFg(50, 0, 100, "#FF0000", "BLUE"));

        t1.formatColumns("X = heatmapFg(100, 0, 100, RED, BLUE)");
        t1.formatColumns("X = heatmapFg(100, 0, 100, `RED`, `BLUE`)");
        t1.formatColumns("X = heatmapFg(100, 0, 100, fg(RED), fg(BLUE))");
        t1.formatColumns("X = heatmapFg(100, 0, 100, fg(`RED`), fg(BLUE))");
        t1.formatColumns("X = heatmapForeground(100, 0, 100, RED, BLUE)");
    }

    public void testToLong() {
        assertEquals(DBColorUtil.bgfga(ALICEBLUE), DBColorUtil.toLong(ALICEBLUE));
        assertEquals(0L, DBColorUtil.toLong((Color) null));
        assertEquals(0L, DBColorUtil.toLong(QueryConstants.NULL_LONG));

        assertEquals(DBColorUtil.bgfga(ALICEBLUE), DBColorUtil.toLong("ALICEBLUE"));
        assertEquals(0L, DBColorUtil.toLong((String) null));

        assertEquals(DBColorUtil.bgfga(ALICEBLUE), DBColorUtil.toLong("#F0F8FF"));
        assertEquals(0L, DBColorUtil.toLong((String) null));

        assertEquals(DBColorUtil.bgfga(ALICEBLUE), DBColorUtil.toLong(Color.color("ALICEBLUE")));
        assertEquals(0L, DBColorUtil.toLong((Color) null));

        assertEquals(DBColorUtil.bgfga(ALICEBLUE), DBColorUtil.toLong(Color.colorRGB(240, 248, 255)));
        assertEquals(0L, DBColorUtil.toLong((Color) null));

        assertEquals(DBColorUtil.bgfga(0, 0, 0), DBColorUtil.toLong(Color.colorRGB(0, 0, 0)));
        assertEquals(0L, DBColorUtil.toLong(Color.NO_FORMATTING));
    }

    public void testIsForegroundSet() {
        assertTrue(DBColorUtil.isForegroundSet(DBColorUtil.fg(240, 248, 255)));
        assertTrue(DBColorUtil.isForegroundSet(DBColorUtil.fg(ALICEBLUE)));
        assertTrue(DBColorUtil.isForegroundSet(DBColorUtil.fg("ALICEBLUE")));
        assertTrue(DBColorUtil.isForegroundSet(DBColorUtil.fg(DBColorUtil.toLong("ALICEBLUE"))));

        assertFalse(DBColorUtil.isForegroundSet(DBColorUtil.bg(240, 248, 255)));
        assertFalse(DBColorUtil.isForegroundSet(DBColorUtil.bg(ALICEBLUE)));
        assertFalse(DBColorUtil.isForegroundSet(DBColorUtil.bg("ALICEBLUE")));
        assertFalse(DBColorUtil.isForegroundSet(DBColorUtil.bg(DBColorUtil.toLong("ALICEBLUE"))));
        assertFalse(DBColorUtil.isForegroundSet(DBColorUtil.toLong(ALICEBLUE)));
        assertFalse(DBColorUtil.isForegroundSet(DBColorUtil.toLong("ALICEBLUE")));
    }

    private void testRowFormatWhere(final Table colorTable, final Color color) {
        final long[] colorTableCol =
                colorTable.getColumn(ColumnFormattingValues.ROW_FORMAT_NAME + ColumnFormattingValues.TABLE_FORMAT_NAME)
                        .getLongs(0, size);

        for (int i = 0; i < 6; i++) {
            // assertEquals(0L, colorTableCol[i]);
        }
        for (int i = 6; i < size; i++) {
            assertEquals(DBColorUtil.toLong(color), colorTableCol[i]);
        }
    }

    private void testFormatColumns(final Table colorTable, final Color color) {
        final long[] colorTableCol =
                colorTable.getColumn("X" + ColumnFormattingValues.TABLE_FORMAT_NAME).getLongs(0, size);
        for (long aColorTableCol : colorTableCol) {
            assertEquals(DBColorUtil.toLong(color), aColorTableCol);
        }
    }
}
