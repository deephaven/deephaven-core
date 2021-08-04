package io.deephaven.integrations.learn;

import io.deephaven.db.tables.Table;
import org.jpy.PyObject;

/**
 * This class creates and adds to this index set when indices are updated or added, and instantiates the Future needed
 * to perform computations on the data at those indices.
 *
 * This is the first layer of this design, as it directly interacts with the dataset to retrieve indices that have been
 * updated or added.
 */

public class Computer {

    final PyObject func;
    final int batchSize;
    final Input[] inputs;
    Future current;

    /**
     * Constructor for Computer. Initializes fields that are needed to instantiate a Future.
     *
     * @param func Python function that the user wants to use on data from a Deephaven table.
     * @param inputs Deephaven Input objects that determine which columns of data get passed to func.
     * @param batchSize Maximum number of rows to perform computations on at once.
     */
    public Computer(PyObject func, Table table, int batchSize, Input ... inputs) {

        // first verify that maxSize is a strictly positive integer
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Max size must be a strictly positive integer.");
        }
        // then verify inputs is non-empty
        if (inputs.length == 0) {
            throw new IllegalArgumentException("Cannot have an empty input list.");
        }
        this.func = func;
        this.batchSize = batchSize;
        this.inputs = inputs;
        // get column source for each input in input array
        for (int i = 0 ; i < this.inputs.length ; i++) {
            this.inputs[i].createColumnSource(table);
        }
        this.current = null;
    }

    /**
     * Resets this Future to null.
     */
    public boolean clear() {
        this.current = null;
        return false;
    }

    /**
     * This method will instantiate a Future if there is not a currently existing one (first call to .compute), or if
     * the current one has an index set that is full. Then, it will add indices for computation to this index set, and
     * defer the actual computation to the .get method of the associated Future.
     *
     * @param k Index to be added to this Future's index set.
     * @return  A Future with the k index added to its index set.
     * @throws Exception Cannot add more indices than the maximum number allowed.
     */
    public Future compute(long k) throws Exception {
        // if current has not been created or its index set is full, create a new one and append appropriate indices
        // and return. Else, just append appropriate indices and return.
        if (this.current == null || this.current.indexSet.isFull()) {
            this.current = new Future(this.func, this.batchSize, this.inputs);
        }
        // add current index to indexSet in Future and return
        this.current.indexSet.add(k);
        return this.current;
    }
    
    // this method has yet to be totally translated from Python, still working on that

    /*
    public List<Input> parseInput(Table table) {
        List<Input> newInputs = null;
        // if this is the case, we assume input is just list of features
        if (this.inputs.size() == 1) {
            // if list provided in input class is empty, replace with all features from table and return
            if (this.inputs.get(0).colNames.size() == 0)  {
                //newInputs.set(0, new Input( table.getMeta().getColumn("Name").getDirect() , this.inputs.get(0).func));
                return newInputs;
            } // else ensure only strings in list, I believe compiler does this?
        } else {
            // verify all elements are of type input, compiler does this?
            // now that we know input length is at least 2, ensure target non-empty
            if (this.inputs.get(0).colNames.size() == 0) {
                throw new ValueException("Target input cannot be empty.");
            } else {
                // create list of targets, I cannot think of any case where this is not length 1
                List<String> target = this.inputs.get(0).colNames;
                // look through every other input to find empty list
                for (int i = 1 ; i < this.inputs.size() ; i++) {
                    // if empty list is found, replace with list of all column names except for targets
                    if (this.inputs.get(i).colSet.size() == 0) {
                        //newInputs.set(i, // same stuff as line 84);
                    } else {
                        // ensure target is not present in any other input list

                    }
                }
            }
        }
        return newInputs;
    }

    public void printMe() {
        System.out.println(this.func);
        for (int i = 0 ; i < this.inputs.length ; i++) {
            System.out.println(this.inputs[i]);
        }
    }

    public Input[] getInputs() {
        return this.inputs;
    }
    */

}
