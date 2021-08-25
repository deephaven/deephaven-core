package io.deephaven.db.v2.select;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.SelectFilterFactory;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

public class TestSelectFilterFactory extends TestCase {

    private static final String STRING_COLUMN = "Strings";
    private static final String INTEGER_COLUMN = "Integers";
    private static final String FLOAT_COLUMN = "Floats";
    private static final String BOOLEAN_COLUMN = "Booleans";

    // For testing end of line escaping
    private static final String NORMAL_STRING = "Hello";
    private static final String NEEDS_ESCAPE = "Hello\n";

    // For testing comma splitting with quotes
    private static final String NO_COMMAS_A = "This String Has No Commas A";
    private static final String NO_COMMAS_B = "This String Has No Commas B";
    private static final String WITH_COMMAS_A = "This, String, Has, Commas, A";
    private static final String WITH_COMMAS_B = "This, String, Has, Commas, B";

    private Table table;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        table = TableTools.newTable(
            TableTools.col(STRING_COLUMN, NORMAL_STRING, NEEDS_ESCAPE, NO_COMMAS_A, NO_COMMAS_B,
                WITH_COMMAS_A, WITH_COMMAS_B),
            TableTools.col(INTEGER_COLUMN, 0, 1, 2, 3, 4, 5),
            TableTools.col(FLOAT_COLUMN, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0),
            TableTools.col(BOOLEAN_COLUMN, true, false, true, false, true, false));
    }

    public void testColumnNameInValueNormal() {
        String value = runSimpleFilterExpresion(" in ", NORMAL_STRING);
        assertEquals(NORMAL_STRING, value);
    }

    public void testColumnNameInValueNeedsEscape() {
        String value = runSimpleFilterExpresion(" in ", NEEDS_ESCAPE);
        assertEquals(NEEDS_ESCAPE, value);
    }

    public void testColumnNameEqualsStringNormal() {
        String value = runSimpleFilterExpresion("==", NORMAL_STRING);
        assertEquals(NORMAL_STRING, value);
    }

    public void testColumnNameEqualsStringNeedsEscape() {
        String value = runSimpleFilterExpresion("==", NEEDS_ESCAPE);
        assertEquals(NEEDS_ESCAPE, value);
    }

    public void testNoEmbeddedCommas() {
        String values = String.join(", ", wrapQuotes(NO_COMMAS_A), wrapBackTicks(NO_COMMAS_B));
        List<Object> result = runDelimitedExpression(STRING_COLUMN, values);
        assertFalse(result.contains(NORMAL_STRING));
        assertFalse(result.contains(NEEDS_ESCAPE));
        assertTrue(result.contains(NO_COMMAS_A));
        assertTrue(result.contains(NO_COMMAS_B));
        assertFalse(result.contains(WITH_COMMAS_A));
        assertFalse(result.contains(WITH_COMMAS_B));
    }

    public void testWithEmbeddedCommas() {
        String values = String.join(", ", wrapQuotes(WITH_COMMAS_A), wrapBackTicks(WITH_COMMAS_B));
        List<Object> result = runDelimitedExpression(STRING_COLUMN, values);
        assertFalse(result.contains(NORMAL_STRING));
        assertFalse(result.contains(NEEDS_ESCAPE));
        assertFalse(result.contains(NO_COMMAS_A));
        assertFalse(result.contains(NO_COMMAS_B));
        assertTrue(result.contains(WITH_COMMAS_A));
        assertTrue(result.contains(WITH_COMMAS_B));
    }

    public void testQuotesMixedCommas() {
        String values = String.join(", ", wrapQuotes(NO_COMMAS_A), wrapQuotes(WITH_COMMAS_A));
        List<Object> result = runDelimitedExpression(STRING_COLUMN, values);
        assertFalse(result.contains(NORMAL_STRING));
        assertFalse(result.contains(NEEDS_ESCAPE));
        assertTrue(result.contains(NO_COMMAS_A));
        assertFalse(result.contains(NO_COMMAS_B));
        assertTrue(result.contains(WITH_COMMAS_A));
        assertFalse(result.contains(WITH_COMMAS_B));
    }

    public void testBackTicksMixedCommas() {
        String values = String.join(", ", wrapBackTicks(NO_COMMAS_A), wrapBackTicks(WITH_COMMAS_A));
        List<Object> result = runDelimitedExpression(STRING_COLUMN, values);
        assertFalse(result.contains(NORMAL_STRING));
        assertFalse(result.contains(NEEDS_ESCAPE));
        assertTrue(result.contains(NO_COMMAS_A));
        assertFalse(result.contains(NO_COMMAS_B));
        assertTrue(result.contains(WITH_COMMAS_A));
        assertFalse(result.contains(WITH_COMMAS_B));
    }

    public void testQuotesAndBackTicksMixedCommas() {
        String values = String.join(", ", wrapQuotes(NO_COMMAS_A), wrapQuotes(WITH_COMMAS_A),
            wrapBackTicks(NO_COMMAS_B), wrapBackTicks(WITH_COMMAS_B));
        List<Object> result = runDelimitedExpression(STRING_COLUMN, values);
        assertFalse(result.contains(NORMAL_STRING));
        assertFalse(result.contains(NEEDS_ESCAPE));
        assertTrue(result.contains(NO_COMMAS_A));
        assertTrue(result.contains(NO_COMMAS_B));
        assertTrue(result.contains(WITH_COMMAS_A));
        assertTrue(result.contains(WITH_COMMAS_B));
    }

    public void testIntegers() {
        List<Object> result = runDelimitedExpression(INTEGER_COLUMN, "1, 3, 5");
        assertFalse(result.contains(0));
        assertTrue(result.contains(1));
        assertFalse(result.contains(2));
        assertTrue(result.contains(3));
        assertFalse(result.contains(4));
        assertTrue(result.contains(5));
    }

    public void testFloats() {
        List<Object> result = runDelimitedExpression(FLOAT_COLUMN, "1.0, 3.0, 5.0");
        assertFalse(result.contains(0.0));
        assertTrue(result.contains(1.0));
        assertFalse(result.contains(2.0));
        assertTrue(result.contains(3.0));
        assertFalse(result.contains(4.0));
        assertTrue(result.contains(5.0));
    }

    public void testBooleans() {
        List<Object> result = runDelimitedExpression(BOOLEAN_COLUMN, "true, false");
        assertTrue(result.contains(true));
        assertTrue(result.contains(false));
    }

    public void testUnmatchedQuoteNoCommas() {
        final String values =
            String.join(", ", unmatchedQuote(NO_COMMAS_A), wrapQuotes(NO_COMMAS_B));
        try {
            runDelimitedExpression(STRING_COLUMN, values);
            fail("Expected FormulaCompilationException");
        } catch (FormulaCompilationException e) {
            // success
        }
    }

    public void testUnmatchedQuoteCommas() {
        final String values =
            String.join(", ", unmatchedQuote(WITH_COMMAS_A), wrapQuotes(WITH_COMMAS_B));
        try {
            runDelimitedExpression(STRING_COLUMN, values);
            fail("Expected FormulaCompilationException");
        } catch (FormulaCompilationException e) {
            // success
        }
    }

    public void testUnmatchedBackTicksNoCommas() {
        final String values =
            String.join(", ", unmatchedBackTick(NO_COMMAS_A), wrapBackTicks(NO_COMMAS_B));
        try {
            runDelimitedExpression(STRING_COLUMN, values);
            fail("Expected FormulaCompilationException");
        } catch (FormulaCompilationException e) {
            // success
        }
    }

    public void testUnmatchedBackTicksCommas() {
        final String values =
            String.join(", ", unmatchedBackTick(WITH_COMMAS_A), wrapBackTicks(WITH_COMMAS_B));
        try {
            runDelimitedExpression(STRING_COLUMN, values);
            fail("Expected FormulaCompilationException");
        } catch (FormulaCompilationException e) {
            // success
        }
    }

    private String runSimpleFilterExpresion(String baseExpresion, String value) {
        String expression = STRING_COLUMN + baseExpresion + wrapBackTicks(value);
        SelectFilter selectFilter = SelectFilterFactory.getExpression(expression);
        selectFilter.init(table.getDefinition());
        Index index = selectFilter.filter(table.getIndex(), table.getIndex(), table, false);
        ColumnSource columnSource = table.getColumnSource(STRING_COLUMN);
        return columnSource.get(index.firstKey()).toString();
    }

    private static String wrapBackTicks(String input) {
        return "`" + input + "`";
    }

    private static String wrapQuotes(String input) {
        return "\"" + input + "\"";
    }

    private static String unmatchedBackTick(String input) {
        return "`" + input;
    }

    private static String unmatchedQuote(String input) {
        return "\"" + input;
    }

    private List<Object> runDelimitedExpression(String columnName, String values) {
        String expression = columnName + " in " + values;
        SelectFilter selectFilter = SelectFilterFactory.getExpression(expression);
        selectFilter.init(table.getDefinition());
        Index index = selectFilter.filter(table.getIndex(), table.getIndex(), table, false);
        ColumnSource columnSource = table.getColumnSource(columnName);
        List<Object> result = new ArrayList<>(index.intSize());
        for (Index.Iterator it = index.iterator(); it.hasNext();) {
            result.add(columnSource.get(it.nextLong()));
        }
        return result;
    }
}
