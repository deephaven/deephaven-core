package io.deephaven.db.plot.datasets;

import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.errors.PlotUnsupportedOperationException;
import io.deephaven.db.plot.util.ArgumentValidations;

import java.io.Serializable;
import java.util.*;

/**
 * Provide unique names for series.
 */
public class DynamicSeriesNamer implements Serializable {

    private static final long serialVersionUID = 1512466762187163906L;

    private final Set<String> names = new LinkedHashSet<>();
    /**
     * This is replica of {@code name}. This will get updated as {@code name} gets updated.
     */
    private final List<String> namesList = new ArrayList<>();
    private final Map<String, Integer> generatedNames = new LinkedHashMap<>();

    /**
     * Adds a new series name to the namer.
     *
     * @param name new series name
     * @throws UnsupportedOperationException if the name has already been added.
     */
    public synchronized void add(final Comparable name, final PlotInfo plotInfo) {
        final String nameString = name.toString();
        final boolean inSet = names.add(nameString);
        if (!inSet) {
            throw new PlotUnsupportedOperationException(
                "Series with the same name already exists in the collection.  name=" + nameString,
                plotInfo);
        } else {
            namesList.add(nameString);
        }
    }

    /**
     * Remove all the specified series names.
     *
     * @param names series names to remove
     */
    public synchronized void removeAll(final Collection<? extends Comparable> names) {
        for (Comparable removedName : names) {
            this.names.remove(removedName.toString());
            this.namesList.remove(removedName.toString());
            this.generatedNames.remove(removedName.toString());
        }
    }

    /**
     * Takes a potential series name and creates a unique name from it. If the series would be new,
     * the original series name is returned.
     *
     * @param potentialName potential series name
     * @return uniquified series name
     */
    public synchronized String makeUnusedName(final String potentialName, final PlotInfo plotInfo) {
        ArgumentValidations.assertNotNull(potentialName, "SeriesName must not be null", plotInfo);
        String tempName = potentialName;

        while (names.contains(tempName)) {
            int index = generatedNames.getOrDefault(potentialName, 1);
            generatedNames.put(potentialName, index + 1);
            tempName = potentialName + " " + index;
        }

        names.add(tempName);
        namesList.add(tempName);
        return tempName;
    }

    /**
     * Gets all the series names. <br/>
     * The order of series in 3D-plots are determined by returned set.
     *
     * @return Set<String> of seriesNames
     */
    public synchronized List<Comparable> getNames() {
        return Collections.unmodifiableList(namesList);
    }

    public synchronized void clear() {
        this.names.clear();
        this.namesList.clear();
        this.generatedNames.clear();
    }
}
