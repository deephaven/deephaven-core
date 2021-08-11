package io.deephaven.integrations.learn;

import io.deephaven.base.verify.Require;

/**
 * Scatterer applies scatter functions to the result of a deferred calculation so that the results can be scattered into new table columns.
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

        this.outputs = outputs;
    }

    /**
     * Applies the scatter function of a particular output to the result of the deferred calculation.
     *
     * @param idx   index of the output to scatter back into the table.
     * @param fo    FutureOffset that contains the results of the deferred calculation as well as the index to access
     *              that calculation.
     * @return subset of result that can be put back into the table.
     */
    public Object scatter(int idx, FutureOffset fo) {
        return outputs[idx].getScatterCaller().apply(new Object[]{fo.getDeferredCalculation(), fo.getOffset()});
    }

    /**
     * Generates query strings to create a new column for each Output.
     *
     * @return list of query strings to be used in .update() call
     */
    public String[] generateQueryStrings() {
        String[] queryStrings = new String[outputs.length];

        for (int i = 0; i < outputs.length; i++) {
            queryStrings[i] = String.format("%s = scatterer.scatter(%d, FutureOffset)", outputs[i].getColName(), i);
        }

        return queryStrings;
    }
}