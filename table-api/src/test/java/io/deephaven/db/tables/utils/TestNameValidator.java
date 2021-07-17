package io.deephaven.db.tables.utils;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestNameValidator {

    private final String inoperable = "*****";
    private final Function<String, String> customReplace = s -> s.replaceAll("\\*", "s");
    private final String nowOperable = "sssss";

    // used in query and column testing
    private final String invalidCol1 = "1Invalid";
    private final String invalidCol2 = "in.valid";
    private final String invalidCol3 = "in-vali3 d";
    private final String reservedCol1 = "i";
    private final String reservedCol2 = "double";
    private final String validCol1 = "t";
    private final String validCol2 = "i2";
    private final String validCol3 = "t2311sfad233safd12";
    private final String validCol4 = "Double";

    // table and namespace testing
    private final String invalidTable1 = "0";
    private final String invalidTable2 = "a0!";
    private final String invalidTable3 = "a0&";
    private final String validTable1 = "A";
    private final String validTable2 = "a0";
    private final String validTable3 = "a0243FDSFDSLKJADS_32-@@@@@++++++asdf321";

    @Test
    public void testColumnNameValidation() {
        // all invalid, but log warnings instead

        // try {
        // DBNameValidator.validateColumnName(invalidCol1);
        // fail("Expected an Exception");
        // } catch(DBNameValidator.InvalidNameException e) {
        // assertTrue(e.getMessage().contains("Invalid"));
        // }
        //
        // try {
        // DBNameValidator.validateColumnName(invalidCol2);
        // fail("Expected an Exception");
        // } catch(DBNameValidator.InvalidNameException e) {
        // assertTrue(e.getMessage().contains("Invalid"));
        // }
        //
        // try {
        // DBNameValidator.validateColumnName(invalidCol3);
        // fail("Expected an Exception");
        // } catch(DBNameValidator.InvalidNameException e) {
        // assertTrue(e.getMessage().contains("Invalid"));
        // }
        //
        // try {
        // DBNameValidator.validateColumnName(reservedCol1);
        // fail("Expected an Exception");
        // } catch(DBNameValidator.InvalidNameException e) {
        // assertTrue(e.getMessage().contains("Invalid"));
        // }
        //
        // try {
        // DBNameValidator.validateColumnName(reservedCol2);
        // fail("Expected an Exception");
        // } catch(DBNameValidator.InvalidNameException e) {
        // assertTrue(e.getMessage().contains("Invalid"));
        // }

        NameValidator.validateColumnName(validCol1);
        NameValidator.validateColumnName(validCol2);
        NameValidator.validateColumnName(validCol3);
        NameValidator.validateColumnName(validCol4);
    }

    @Test
    public void testQueryNameValidation() {
        // all invalid

        try {
            NameValidator.validateQueryParameterName(invalidCol1);
            fail("Expected an Exception");
        } catch (NameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        try {
            NameValidator.validateQueryParameterName(invalidCol2);
            fail("Expected an Exception");
        } catch (NameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        try {
            NameValidator.validateQueryParameterName(invalidCol3);
            fail("Expected an Exception");
        } catch (NameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        try {
            NameValidator.validateQueryParameterName(reservedCol1);
            fail("Expected an Exception");
        } catch (NameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        try {
            NameValidator.validateQueryParameterName(reservedCol2);
            fail("Expected an Exception");
        } catch (NameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        NameValidator.validateQueryParameterName(validCol1);
        NameValidator.validateQueryParameterName(validCol2);
        NameValidator.validateQueryParameterName(validCol3);
        NameValidator.validateQueryParameterName(validCol4);
    }

    @Test
    public void testTableNameValidation() {
        try {
            NameValidator.validateTableName(invalidTable1);
            fail("Expected an Exception");
        } catch (NameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        try {
            NameValidator.validateTableName(invalidTable2);
            fail("Expected an Exception");
        } catch (NameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        try {
            NameValidator.validateTableName(invalidTable3);
            fail("Expected an Exception");
        } catch (NameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        NameValidator.validateTableName(validTable1);
        NameValidator.validateTableName(validTable2);
        NameValidator.validateTableName(validTable3);
    }

    @Test
    public void testNamespaceNameValidation() {
        final String invalidTable1 = "0";
        final String invalidTable2 = "a0!";
        final String invalidTable3 = "a0&";

        final String validTable1 = "A";
        final String validTable2 = "a0";
        final String validTable3 = "a0243FDSFDSLKJADS_32-@@@@@++++++asdf321";

        try {
            NameValidator.validateNamespaceName(invalidTable1);
            fail("Expected an Exception");
        } catch (NameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        try {
            NameValidator.validateNamespaceName(invalidTable2);
            fail("Expected an Exception");
        } catch (NameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        try {
            NameValidator.validateNamespaceName(invalidTable3);
            fail("Expected an Exception");
        } catch (NameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        NameValidator.validateNamespaceName(validTable1);
        NameValidator.validateNamespaceName(validTable2);
        NameValidator.validateNamespaceName(validTable3);
    }

    @Test
    public void testPartitionNameValidation() {
        final String invalidTable1 = "0 df";
        final String invalidTable2 = "_";
        final String invalidTable3 = "a0&";

        final String validTable1 = "A";
        final String validTable2 = "0";
        final String validTable3 = "0A0";
        final String validTable4 = "a0243FDSFDSLKJADS_32-@@@@@++++++asdf321";

        try {
            NameValidator.validatePartitionName(invalidTable1);
            fail("Expected an Exception");
        } catch (NameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        try {
            NameValidator.validatePartitionName(invalidTable2);
            fail("Expected an Exception");
        } catch (NameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        try {
            NameValidator.validatePartitionName(invalidTable3);
            fail("Expected an Exception");
        } catch (NameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }

        NameValidator.validatePartitionName(validTable1);
        NameValidator.validatePartitionName(validTable2);
        NameValidator.validatePartitionName(validTable3);
        NameValidator.validatePartitionName(validTable4);
    }

    @Test
    public void testLegalizeColumnName() {
        String validated = NameValidator.legalizeColumnName(invalidCol1);
        assertEquals(validated, "column_1Invalid");

        validated = NameValidator.legalizeColumnName(invalidCol2);
        assertEquals(validated, "invalid");

        Set<String> taken = new HashSet<>();
        taken.add("invali3d");
        validated = NameValidator.legalizeColumnName(invalidCol3, taken);
        assertEquals(validated, "invali3d2");

        validated = NameValidator.legalizeColumnName(reservedCol1);
        assertEquals(validated, "column_i");

        validated = NameValidator.legalizeColumnName(reservedCol2);
        assertEquals(validated, "column_double");

        validated = NameValidator.legalizeColumnName(validCol1);
        assertEquals(validated, validCol1);

        validated = NameValidator.legalizeColumnName(validCol2);
        assertEquals(validated, validCol2);

        validated = NameValidator.legalizeColumnName(validCol3);
        assertEquals(validated, validCol3);

        validated = NameValidator.legalizeColumnName(validCol4);
        assertEquals(validated, validCol4);

        try {
            NameValidator.legalizeColumnName(inoperable);
            fail("Expected an Exception");
        } catch (NameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("legalize"));
        }

        validated = NameValidator.legalizeColumnName(inoperable, customReplace);
        assertEquals(validated, nowOperable);
    }

    @Test
    public void testLegalizeQueryName() {
        String validated = NameValidator.legalizeQueryParameterName(invalidCol1);
        assertEquals(validated, "var_1Invalid");

        validated = NameValidator.legalizeQueryParameterName(invalidCol2);
        assertEquals(validated, "invalid");

        Set<String> taken = new HashSet<>();
        taken.add("invali3d");
        validated = NameValidator.legalizeQueryParameterName(invalidCol3, taken);
        assertEquals(validated, "invali3d2");

        validated = NameValidator.legalizeQueryParameterName(reservedCol1);
        assertEquals(validated, "var_i");

        validated = NameValidator.legalizeQueryParameterName(reservedCol2);
        assertEquals(validated, "var_double");

        validated = NameValidator.legalizeQueryParameterName(validCol1);
        assertEquals(validated, validCol1);

        validated = NameValidator.legalizeQueryParameterName(validCol2);
        assertEquals(validated, validCol2);

        validated = NameValidator.legalizeQueryParameterName(validCol3);
        assertEquals(validated, validCol3);

        validated = NameValidator.legalizeQueryParameterName(validCol4);
        assertEquals(validated, validCol4);

        try {
            NameValidator.legalizeQueryParameterName(inoperable);
            fail("Expected an Exception");
        } catch (NameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("legalize"));
        }

        validated = NameValidator.legalizeQueryParameterName(inoperable, customReplace);
        assertEquals(validated, nowOperable);
    }

    @Test
    public void testLegalizeTableName() {
        String validated = NameValidator.legalizeTableName(invalidTable1);
        assertEquals(validated, "table_0");

        validated = NameValidator.legalizeTableName(invalidTable2);
        assertEquals(validated, "a0");

        Set<String> taken = new HashSet<>();
        taken.add(validated);
        validated = NameValidator.legalizeTableName(invalidTable3, taken);
        assertEquals(validated, "a02");

        validated = NameValidator.legalizeTableName(validTable1);
        assertEquals(validated, validTable1);

        validated = NameValidator.legalizeTableName(validTable2);
        assertEquals(validated, validTable2);

        validated = NameValidator.legalizeTableName(validTable3);
        assertEquals(validated, validTable3);

        try {
            NameValidator.legalizeTableName(inoperable);
            fail("Expected an Exception");
        } catch (NameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("legalize"));
        }

        validated = NameValidator.legalizeTableName(inoperable, customReplace);
        assertEquals(validated, nowOperable);
    }

    @Test
    public void testLegalizeNamespaceName() {
        try {
            NameValidator.legalizeNamespaceName(invalidTable1);
            fail("Expected an Exception");
        } catch (NameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("legalize"));
        }

        String validated = NameValidator.legalizeNamespaceName(invalidTable2);
        assertEquals(validated, "a0");

        Set<String> taken = new HashSet<>();
        taken.add(validated);
        validated = NameValidator.legalizeNamespaceName(invalidTable3, taken);
        assertEquals(validated, "a02");

        validated = NameValidator.legalizeNamespaceName(validTable1);
        assertEquals(validated, validTable1);

        validated = NameValidator.legalizeNamespaceName(validTable2);
        assertEquals(validated, validTable2);

        validated = NameValidator.legalizeNamespaceName(validTable3);
        assertEquals(validated, validTable3);

        try {
            NameValidator.legalizeNamespaceName(inoperable);
            fail("Expected an Exception");
        } catch (NameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("legalize"));
        }

        validated = NameValidator.legalizeNamespaceName(inoperable, customReplace);
        assertEquals(validated, nowOperable);
    }

    @Test
    public void testLegalizeNames() {
        final String[] canBeLegalized1 = {invalidCol1, invalidCol2, invalidCol3, reservedCol1,
                reservedCol2, validCol1, validCol2, validCol3, validCol4};

        final String[] canBeLegalized1Dupes = {invalidCol1, invalidCol2, invalidCol3, invalidCol3,
                reservedCol1, reservedCol2, validCol1, validCol2, validCol3, validCol4};

        final String[] cantBeLegalized1 = {invalidCol1, invalidCol2, invalidCol3, reservedCol1,
                reservedCol2, validCol1, validCol2, validCol3, validCol4, inoperable};

        // columnnames
        String[] ret = NameValidator.legalizeColumnNames(canBeLegalized1);
        String[] correct = {"column_1Invalid", "invalid", "invali3d", "column_i", "column_double",
                validCol1, validCol2, validCol3, validCol4};
        assertArrayEquals(ret, correct);

        ret = NameValidator.legalizeColumnNames(canBeLegalized1Dupes, true);
        correct = new String[] {"column_1Invalid", "invalid", "invali3d", "invali3d2", "column_i",
                "column_double", validCol1, validCol2, validCol3, validCol4};
        assertArrayEquals(ret, correct);

        try {
            NameValidator.legalizeColumnNames(canBeLegalized1Dupes);
            fail("Expected an Exception");
        } catch (NameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("Duplicate"));
        }

        // query parameter names
        ret = NameValidator.legalizeQueryParameterNames(canBeLegalized1);
        correct = new String[] {"var_1Invalid", "invalid", "invali3d", "var_i", "var_double",
                validCol1, validCol2, validCol3, validCol4};
        assertArrayEquals(ret, correct);

        ret = NameValidator.legalizeQueryParameterNames(canBeLegalized1Dupes, true);
        correct = new String[] {"var_1Invalid", "invalid", "invali3d", "invali3d2", "var_i",
                "var_double", validCol1, validCol2, validCol3, validCol4};
        assertArrayEquals(ret, correct);

        try {
            NameValidator.legalizeQueryParameterNames(canBeLegalized1Dupes);
            fail("Expected an Exception");
        } catch (NameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("Duplicate"));
        }

        final String[] canBeLegalized2 =
            {invalidTable1, invalidTable2, invalidTable3, validTable1, validTable2, validTable3};

        // table names
        try {
            NameValidator.legalizeTableNames(canBeLegalized2);
            fail("Expected an Exception");
        } catch (NameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("Duplicate"));
        }

        ret = NameValidator.legalizeTableNames(canBeLegalized2, true);
        correct = new String[] {"table_0", "a0", "a02", validTable1, "a03", validTable3};
        assertArrayEquals(ret, correct);

        // namespace names
        try {
            NameValidator.legalizeNamespaceNames(canBeLegalized2);
            fail("Expected an Exception");
        } catch (NameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("legalize"));
        }

        final String[] canBeLegalized3 =
            {invalidTable2, invalidTable3, validTable1, validTable2, validTable3};

        ret = NameValidator.legalizeNamespaceNames(canBeLegalized3, true);
        correct = new String[] {"a0", "a02", validTable1, "a03", validTable3};
        assertArrayEquals(ret, correct);
    }
}
