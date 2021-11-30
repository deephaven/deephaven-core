package io.deephaven.plot;

/**
 * A factory for creating a new Figure.
 */
public class FigureFactory {
    private FigureFactory() {}

    /**
     * Creates a new figure.
     *
     * @return new figure
     */
    public static Figure figure() {
        return new FigureImpl();
    }

    /**
     * Creates a new figure.
     *
     * @param numRows number or rows in the figure grid.
     * @param numCols number or columns in the figure grid.
     * @return new figure
     */
    public static Figure figure(final int numRows, final int numCols) {
        return new FigureImpl(numRows, numCols);
    }

}
