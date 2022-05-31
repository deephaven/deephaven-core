package io.deephaven.plot.example_plots;

import io.deephaven.plot.BaseFigureImpl;
import io.deephaven.plot.Figure;
import io.deephaven.plot.FigureImpl;

/**
 * Utilities for rendering example plots.
 */
public class ExamplePlotUtils {

    /**
     * Creates a frame to display a figure
     * 
     * @param fig figure
     * @return frame displaying the figure
     */
    public static void display(final Figure fig) {
        final BaseFigureImpl figImpl = ((FigureImpl) fig).getFigure();
        // TODO: Do something here to actually display a sample plot, in the absence of Swing plotting support.
    }
}
