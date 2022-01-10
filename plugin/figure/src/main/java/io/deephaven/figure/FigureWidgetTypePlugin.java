package io.deephaven.figure;

import io.deephaven.plot.FigureWidget;
import io.deephaven.plugin.type.ObjectTypeClassBase;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An object type named {@value NAME} of java class type {@link FigureWidget}.
 */
public final class FigureWidgetTypePlugin extends ObjectTypeClassBase<FigureWidget> {

    private static final FigureWidgetTypePlugin INSTANCE = new FigureWidgetTypePlugin();

    public static final String NAME = "Figure";

    public static FigureWidgetTypePlugin instance() {
        return INSTANCE;
    }

    private FigureWidgetTypePlugin() {
        super(NAME, FigureWidget.class);
    }

    @Override
    public void writeToImpl(Exporter exporter, FigureWidget figureWidget, OutputStream out) throws IOException {
        FigureWidgetTranslator.translate(figureWidget, exporter).writeTo(out);
    }
}
