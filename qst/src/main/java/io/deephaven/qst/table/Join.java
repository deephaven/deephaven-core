package io.deephaven.qst.table;

import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;

import java.util.List;

public interface Join extends TableSpec {

    TableSpec left();

    TableSpec right();

    List<JoinMatch> matches();

    List<JoinAddition> additions();

    interface Builder<J extends Join, SELF extends Builder<J, SELF>> {
        SELF left(TableSpec left);

        SELF right(TableSpec right);

        SELF addMatches(JoinMatch element);

        SELF addMatches(JoinMatch... elements);

        SELF addAllMatches(Iterable<? extends JoinMatch> elements);

        SELF addAdditions(JoinAddition element);

        SELF addAdditions(JoinAddition... elements);

        SELF addAllAdditions(Iterable<? extends JoinAddition> elements);

        J build();
    }
}
