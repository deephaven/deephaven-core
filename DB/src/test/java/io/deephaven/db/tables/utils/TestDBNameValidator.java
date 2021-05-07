package io.deephaven.db.tables.utils;

import io.deephaven.base.testing.BaseArrayTestCase;
import junit.framework.TestCase;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public class TestDBNameValidator extends BaseArrayTestCase {

    private final String inoperable = "*****";
    private final Function<String, String> customReplace = s -> s.replaceAll("\\*", "s");
    private final String nowOperable = "sssss";

    //used in query and column testing
    private final String invalidCol1 = "1Invalid";
    private final String invalidCol2 = "in.valid";
    private final String invalidCol3 = "in-vali3 d";
    private final String reservedCol1 = "i";
    private final String reservedCol2 = "double";
    private final String validCol1 = "t";
    private final String validCol2 = "i2";
    private final String validCol3 = "t2311sfad233safd12";
    private final String validCol4 = "Double";

    //table and namespace testing
    private final String invalidTable1 = "0";
    private final String invalidTable2 = "a0!";
    private final String invalidTable3 = "a0&";
    private final String validTable1 = "A";
    private final String validTable2 = "a0";
    private final String validTable3 = "a0243FDSFDSLKJADS_32-@@@@@++++++asdf321";

    public void testColumnNameValidation() {
        //all invalid, but log warnings instead
        /*
        try {
            DBNameValidator.validateColumnName(invalidCol1);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }


        try {
            DBNameValidator.validateColumnName(invalidCol2);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }



        try {
            DBNameValidator.validateColumnName(invalidCol3);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }



        try {
            DBNameValidator.validateColumnName(reservedCol1);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }



        try {
            DBNameValidator.validateColumnName(reservedCol2);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }
        */



        DBNameValidator.validateColumnName(validCol1);
        DBNameValidator.validateColumnName(validCol2);
        DBNameValidator.validateColumnName(validCol3);
        DBNameValidator.validateColumnName(validCol4);
    }

    public void testQueryNameValidation() {
        //all invalid

        try {
            DBNameValidator.validateQueryParameterName(invalidCol1);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }



        try {
            DBNameValidator.validateQueryParameterName(invalidCol2);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }



        try {
            DBNameValidator.validateQueryParameterName(invalidCol3);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }



        try {
            DBNameValidator.validateQueryParameterName(reservedCol1);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }



        try {
            DBNameValidator.validateQueryParameterName(reservedCol2);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }



        DBNameValidator.validateQueryParameterName(validCol1);
        DBNameValidator.validateQueryParameterName(validCol2);
        DBNameValidator.validateQueryParameterName(validCol3);
        DBNameValidator.validateQueryParameterName(validCol4);
    }

    public void testTableNameValidation() {
        try {
            DBNameValidator.validateTableName(invalidTable1);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }



        try {
            DBNameValidator.validateTableName(invalidTable2);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }



        try {
            DBNameValidator.validateTableName(invalidTable3);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }



        DBNameValidator.validateTableName(validTable1);
        DBNameValidator.validateTableName(validTable2);
        DBNameValidator.validateTableName(validTable3);
    }

    public void testNamespaceNameValidation() {
        final String invalidTable1 = "0";
        final String invalidTable2 = "a0!";
        final String invalidTable3 = "a0&";


        final String validTable1 = "A";
        final String validTable2 = "a0";
        final String validTable3 = "a0243FDSFDSLKJADS_32-@@@@@++++++asdf321";


        try {
            DBNameValidator.validateNamespaceName(invalidTable1);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }



        try {
            DBNameValidator.validateNamespaceName(invalidTable2);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }



        try {
            DBNameValidator.validateNamespaceName(invalidTable3);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }



        DBNameValidator.validateNamespaceName(validTable1);
        DBNameValidator.validateNamespaceName(validTable2);
        DBNameValidator.validateNamespaceName(validTable3);
    }

    public void testPartitionNameValidation() {
        final String invalidTable1 = "0 df";
        final String invalidTable2 = "_";
        final String invalidTable3 = "a0&";


        final String validTable1 = "A";
        final String validTable2 = "0";
        final String validTable3 = "0A0";
        final String validTable4 = "a0243FDSFDSLKJADS_32-@@@@@++++++asdf321";


        try {
            DBNameValidator.validatePartitionName(invalidTable1);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }



        try {
            DBNameValidator.validatePartitionName(invalidTable2);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }



        try {
            DBNameValidator.validatePartitionName(invalidTable3);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.InvalidNameException e) {
            assertTrue(e.getMessage().contains("Invalid"));
        }



        DBNameValidator.validatePartitionName(validTable1);
        DBNameValidator.validatePartitionName(validTable2);
        DBNameValidator.validatePartitionName(validTable3);
        DBNameValidator.validatePartitionName(validTable4);
    }

    public void testLegalizeColumnName() {
        String validated = DBNameValidator.legalizeColumnName(invalidCol1);
        assertEquals(validated, "column_1Invalid");

        validated = DBNameValidator.legalizeColumnName(invalidCol2);
        assertEquals(validated, "invalid");

        Set<String> taken = new HashSet<>();
        taken.add("invali3d");
        validated = DBNameValidator.legalizeColumnName(invalidCol3, taken);
        assertEquals(validated, "invali3d2");

        validated = DBNameValidator.legalizeColumnName(reservedCol1);
        assertEquals(validated, "column_i");

        validated = DBNameValidator.legalizeColumnName(reservedCol2);
        assertEquals(validated, "column_double");

        validated = DBNameValidator.legalizeColumnName(validCol1);
        assertEquals(validated, validCol1);

        validated = DBNameValidator.legalizeColumnName(validCol2);
        assertEquals(validated, validCol2);

        validated = DBNameValidator.legalizeColumnName(validCol3);
        assertEquals(validated, validCol3);

        validated = DBNameValidator.legalizeColumnName(validCol4);
        assertEquals(validated, validCol4);

        try {
            DBNameValidator.legalizeColumnName(inoperable);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("legalize"));
        }

        validated = DBNameValidator.legalizeColumnName(inoperable, customReplace);
        assertEquals(validated, nowOperable);
    }

    public void testLegalizeQueryName() {
        String validated = DBNameValidator.legalizeQueryParameterName(invalidCol1);
        assertEquals(validated, "var_1Invalid");

        validated = DBNameValidator.legalizeQueryParameterName(invalidCol2);
        assertEquals(validated, "invalid");

        Set<String> taken = new HashSet<>();
        taken.add("invali3d");
        validated = DBNameValidator.legalizeQueryParameterName(invalidCol3, taken);
        assertEquals(validated, "invali3d2");

        validated = DBNameValidator.legalizeQueryParameterName(reservedCol1);
        assertEquals(validated, "var_i");

        validated = DBNameValidator.legalizeQueryParameterName(reservedCol2);
        assertEquals(validated, "var_double");

        validated = DBNameValidator.legalizeQueryParameterName(validCol1);
        assertEquals(validated, validCol1);

        validated = DBNameValidator.legalizeQueryParameterName(validCol2);
        assertEquals(validated, validCol2);

        validated = DBNameValidator.legalizeQueryParameterName(validCol3);
        assertEquals(validated, validCol3);

        validated = DBNameValidator.legalizeQueryParameterName(validCol4);
        assertEquals(validated, validCol4);

        try {
            DBNameValidator.legalizeQueryParameterName(inoperable);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("legalize"));
        }

        validated = DBNameValidator.legalizeQueryParameterName(inoperable, customReplace);
        assertEquals(validated, nowOperable);
    }

    public void testLegalizeTableName() {
        String validated = DBNameValidator.legalizeTableName(invalidTable1);
        assertEquals(validated, "table_0");

        validated = DBNameValidator.legalizeTableName(invalidTable2);
        assertEquals(validated, "a0");

        Set<String> taken = new HashSet<>();
        taken.add(validated);
        validated = DBNameValidator.legalizeTableName(invalidTable3, taken);
        assertEquals(validated, "a02");

        validated = DBNameValidator.legalizeTableName(validTable1);
        assertEquals(validated, validTable1);

        validated = DBNameValidator.legalizeTableName(validTable2);
        assertEquals(validated, validTable2);

        validated = DBNameValidator.legalizeTableName(validTable3);
        assertEquals(validated, validTable3);

        try {
            DBNameValidator.legalizeTableName(inoperable);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("legalize"));
        }

        validated = DBNameValidator.legalizeTableName(inoperable, customReplace);
        assertEquals(validated, nowOperable);
    }

    public void testLegalizeNamespaceName() {
        try {
            DBNameValidator.legalizeNamespaceName(invalidTable1);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("legalize"));
        }

        String validated = DBNameValidator.legalizeNamespaceName(invalidTable2);
        assertEquals(validated, "a0");

        Set<String> taken = new HashSet<>();
        taken.add(validated);
        validated = DBNameValidator.legalizeNamespaceName(invalidTable3, taken);
        assertEquals(validated, "a02");

        validated = DBNameValidator.legalizeNamespaceName(validTable1);
        assertEquals(validated, validTable1);

        validated = DBNameValidator.legalizeNamespaceName(validTable2);
        assertEquals(validated, validTable2);

        validated = DBNameValidator.legalizeNamespaceName(validTable3);
        assertEquals(validated, validTable3);

        try {
            DBNameValidator.legalizeNamespaceName(inoperable);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("legalize"));
        }

        validated = DBNameValidator.legalizeNamespaceName(inoperable, customReplace);
        assertEquals(validated, nowOperable);
    }

    public void testLegalizeNames() {
        final String[] canBeLegalized1 = {invalidCol1, invalidCol2, invalidCol3, reservedCol1, reservedCol2,
                validCol1, validCol2, validCol3, validCol4};

        final String[] canBeLegalized1Dupes = {invalidCol1, invalidCol2, invalidCol3, invalidCol3, reservedCol1, reservedCol2,
                validCol1, validCol2, validCol3, validCol4};

        final String[] cantBeLegalized1 = {invalidCol1, invalidCol2, invalidCol3, reservedCol1, reservedCol2,
                validCol1, validCol2, validCol3, validCol4, inoperable};

        //columnnames
        String[] ret = DBNameValidator.legalizeColumnNames(canBeLegalized1);
        String[] correct = {"column_1Invalid", "invalid", "invali3d", "column_i",  "column_double",
                validCol1, validCol2, validCol3, validCol4};
        assertEquals(ret, correct);

        ret = DBNameValidator.legalizeColumnNames(canBeLegalized1Dupes, true);
        correct = new String[]{"column_1Invalid", "invalid", "invali3d", "invali3d2", "column_i",  "column_double",
                validCol1, validCol2, validCol3, validCol4};
        assertEquals(ret, correct);

        try {
            DBNameValidator.legalizeColumnNames(canBeLegalized1Dupes);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("Duplicate"));
        }


        //query parameter names
        ret = DBNameValidator.legalizeQueryParameterNames(canBeLegalized1);
        correct = new String[]{"var_1Invalid", "invalid", "invali3d", "var_i",  "var_double",
                validCol1, validCol2, validCol3, validCol4};
        assertEquals(ret, correct);

        ret = DBNameValidator.legalizeQueryParameterNames(canBeLegalized1Dupes, true);
        correct = new String[]{"var_1Invalid", "invalid", "invali3d", "invali3d2", "var_i",  "var_double",
                validCol1, validCol2, validCol3, validCol4};
        assertEquals(ret, correct);

        try {
            DBNameValidator.legalizeQueryParameterNames(canBeLegalized1Dupes);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("Duplicate"));
        }



        final String[] canBeLegalized2 = {invalidTable1, invalidTable2, invalidTable3, validTable1, validTable2,
                validTable3};

        //table names
        try {
            DBNameValidator.legalizeTableNames(canBeLegalized2);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("Duplicate"));
        }

        ret = DBNameValidator.legalizeTableNames(canBeLegalized2, true);
        correct = new String[]{"table_0", "a0", "a02", validTable1, "a03", validTable3};
        assertEquals(ret, correct);


        //namespace names
        try {
            DBNameValidator.legalizeNamespaceNames(canBeLegalized2);
            TestCase.fail("Expected an Exception");
        } catch(DBNameValidator.LegalizeNameException e) {
            assertTrue(e.getMessage().contains("legalize"));
        }

        final String[] canBeLegalized3 = {invalidTable2, invalidTable3, validTable1, validTable2,
                validTable3};

        ret = DBNameValidator.legalizeNamespaceNames(canBeLegalized3, true);
        correct = new String[]{"a0", "a02", validTable1, "a03", validTable3};
        assertEquals(ret, correct);
    }
}
