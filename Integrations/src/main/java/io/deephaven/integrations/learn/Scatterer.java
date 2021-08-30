package io.deephaven.integrations.learn;

import io.deephaven.base.verify.Require;

/**
 * Scatterer applies scatter functions to the result of a deferred calculation so that the results can be scattered into
 * new table columns.
 */
public class Scatterer {

    private final Output[] outputs;

    /**
     * Creates a new Scatterer.
     *
     * @param outputs array of Outputs that determine how data will be scattered back to the table.
     */
    public Scatterer(Output[] outputs) {

        Require.neqNull(outputs, "outputs");
        Require.elementsNeqNull(outputs, "outputs");

        this.outputs = outputs;
    }

    /**
     * Applies the scatter function of each output to the result of a deferred calculation to get the result into a
     * column.
     *
     * @param idx index of the particular output in the list of outputs to use for scattering.
     * @param fo FutureOffset that contains the results of the deferred calculation as well as the index of the row.
     *        that calculation belongs to.
     * @return result of the deferred calculation to be stored into a column.
     */
    public Object scatter(int idx, FutureOffset fo) {
        return outputs[idx].getScatterFunc().apply(new Object[] {fo.getFuture().get(), fo.getOffset()});
    }

    /**
     * Generates query strings to create a new column for each Output.
     *
     * @param futureOffsetColName name of the FutureOffset column to get results from
     *
     * @return list of query strings to be used in .update() call.
     */
    public String[] generateQueryStrings(String futureOffsetColName) {
        String[] queryStrings = new String[outputs.length];

        for (int i = 0; i < outputs.length; i++) {
            // TODO: TICKET #1026: Replace typeString with the following once ticket #1009 is resolved:
            // final String typeString = outputs[i].getType() == null ? "" : "(" + outputs[i].getType() + ")";

            final String typeString = "";
            queryStrings[i] = String.format("%s = %s (__scatterer.scatter(%d, %s))", outputs[i].getColName(),
                    typeString, i, futureOffsetColName);
        }

        return queryStrings;
    }
}
