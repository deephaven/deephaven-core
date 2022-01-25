package io.deephaven.figure;

import com.google.auto.service.AutoService;
import io.deephaven.plot.FigureWidget;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeClassBase;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An object type named {@value NAME} of java class type {@link FigureWidget}.
 */
@AutoService(ObjectType.class)
public final class FigureWidgetTypePlugin extends ObjectTypeClassBase<FigureWidget> {

    public static final String NAME = "Figure";

    public FigureWidgetTypePlugin() {
        super(NAME, FigureWidget.class);
    }

    @Override
    public void writeToImpl(Exporter exporter, FigureWidget figureWidget, OutputStream out) throws IOException {
        FigureWidgetTranslator.translate(figureWidget, exporter).writeTo(out);
    }
}
