package io.deephaven.web.shared.data;

import java.io.Serializable;
import java.util.*;

/**
 * We still send plain strings to the server and receive plain strings from the client.
 *
 * This is here mostly to have a sane place to handle client introspection of custom column definitions.
 *
 * We should probably wire this into place for our internal guts, and convert to this form immediately upon receiving
 * input from user.
 *
 */
public class CustomColumnDescriptor implements Serializable {

    private static final String VALID_ID_REGEX = "^[a-zA-Z_$][a-zA-Z0-9_$]*$";
    private String expression;
    private transient String name;

    /**
     * Extracts the column name from a given column expression.
     *
     * Based on the logic in io.deephaven.engine.tables.select.SelectColumnFactory, the valid expressions take the form:
     * 
     * <pre>
     *     <ColumnName>
     *     <ColumnName>=<Expression>
     *     last <ColumnName>
     *     last(<ColumnName>)
     * </pre>
     *
     * So, we can safely extract a column name for this to have some semblance of identity semantics for custom column
     * definitions.
     *
     * Also, we are explicitly *NOT* supporting deprecated last() syntax, so it will be ignored.
     *
     * @param expression A valid column expression. We perform no validation beyond an assertion on the resulting name.
     * @return A valid column name if the input column expression is itself valid.
     */
    private static String extractColumnName(String expression) {
        expression = expression.trim();
        String result = expression.split("=")[0].trim();
        assert result.matches(VALID_ID_REGEX) : "Invalid column name " + result + " extracted from " + expression;
        return result;
    }

    public String getExpression() {
        return expression;
    }

    public CustomColumnDescriptor setExpression(String expression) {
        this.expression = expression;
        this.name = null; // recalculated on the fly
        return this;
    }

    public String getName() {
        if (name == null) {
            name = extractColumnName(expression);
        }
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final CustomColumnDescriptor that = (CustomColumnDescriptor) o;

        return Objects.equals(expression, that.expression) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, name);
    }

    public static boolean isCompatible(List<CustomColumnDescriptor> was, List<CustomColumnDescriptor> is) {
        HashSet<String> existing = new HashSet<>();
        for (CustomColumnDescriptor col : was) {
            existing.add(col.getName());
        }
        for (CustomColumnDescriptor col : is) {
            existing.remove(col.getName());
        }
        return existing.isEmpty();
    }

    public static List<CustomColumnDescriptor> from(String[] newCustomColumns) {
        final Set<String> descriptorNames = new HashSet<>();
        final List<CustomColumnDescriptor> list = new ArrayList<>();
        for (String col : newCustomColumns) {
            CustomColumnDescriptor descriptor = new CustomColumnDescriptor().setExpression(col);
            if (!descriptorNames.add(descriptor.getName())) {
                throw new IllegalArgumentException("Duplicate custom column: " + descriptor.getName());
            }
            list.add(descriptor);
        }
        return list;
    }

    @Override
    public String toString() {
        return "CustomColumnDescriptor { " + expression + " }";
    }
}
