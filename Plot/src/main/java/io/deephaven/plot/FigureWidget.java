/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot;

import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.util.FigureWidgetMarker;
import io.deephaven.engine.util.LiveWidget;
import io.deephaven.engine.util.LiveWidgetVisibilityProvider;
import io.deephaven.util.annotations.ScriptApi;

import java.util.*;

/**
 * Displayable version of a Figure.
 */
public class FigureWidget extends FigureImpl implements LiveWidget, LiveWidgetVisibilityProvider, FigureWidgetMarker {

    private static final long serialVersionUID = 763409998768966385L;
    private String[] validGroups;

    @SuppressWarnings("WeakerAccess") // this is used in the python integration
    public FigureWidget(final FigureImpl figure) {
        super(figure);
        figure.getFigure().consolidateTableMaps();
    }

    @ScriptApi
    @Override
    public String[] getValidGroups() {
        return validGroups;
    }

    @ScriptApi
    public void setValidGroups(final String... validGroups) {
        this.validGroups = validGroups;
    }

    @ScriptApi
    public void setValidGroups(final Collection<String> validGroups) {
        setValidGroups(validGroups.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
    }
}
