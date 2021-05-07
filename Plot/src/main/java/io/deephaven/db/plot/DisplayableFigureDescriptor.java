package io.deephaven.db.plot;

import io.deephaven.db.plot.errors.PlotInfo;
import io.deephaven.db.plot.util.tables.TableHandle;
import io.deephaven.db.plot.util.tables.TableMapHandle;
import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.TableMap;
import io.deephaven.db.v2.TableMapSupplier;

import java.util.*;

/**
 * Descriptor for a displayable figure, to be used for describing a server-side figure to clients.
 */
public class DisplayableFigureDescriptor {

    private final FigureImpl figure;

    private final List<Table> tables = new ArrayList<>();
    private final List<TableMap> tableMaps = new ArrayList<>();
    private final List<Set<Integer>> tableIds = new ArrayList<>();
    private final List<Set<Integer>> tableMapIds = new ArrayList<>();

    public DisplayableFigureDescriptor(final FigureImpl figure) {
        this.figure = figure;
        consolidateHandles(figure.getFigure().getTableHandles(), figure.getFigure().getTableMapHandles());
    }

    private void consolidateHandles(final Set<TableHandle> tableHandles, final Set<TableMapHandle> tableMapHandles) {
        // region Table Consolidation
        // The first step here is to figure out all of the needed columns for all possible table handles, then
        // reduce the column set to the needed columns.
        final Map<Table, Set<Integer>> tableIdMap = new IdentityHashMap<>();
        final Map<Table, Set<String>> tableColumnMap = new IdentityHashMap<>();

        for (final TableHandle h : tableHandles) {
            tableColumnMap.computeIfAbsent(h.getTable(), t -> new HashSet<>()).addAll(h.getColumns());
            tableIdMap.computeIfAbsent(h.getTable(), t -> new HashSet<>()).add(h.id());
        }

        for (final Map.Entry<Table, Set<Integer>> entry : tableIdMap.entrySet()) {
            Table table = entry.getKey();

            final Set<String> relevantColumns = tableColumnMap.get(table);
            table = table.view(relevantColumns);

            tables.add(table);
            tableIds.add(entry.getValue());
        }
        //endregion

        //region TableMap Consolidation
        // Retrieval of data from table maps will always automatically select columns of importance.
        final Map<TableMap, Set<Integer>> tableMapIdMap = new IdentityHashMap<>();
        final Map<TableMap, Set<String>> tableMapColumnMap = new IdentityHashMap<>();

        for (final TableMapHandle h : tableMapHandles) {
            tableMapIdMap.computeIfAbsent(h.getTableMap(), t -> new HashSet<>()).add(h.id());
            tableMapColumnMap.computeIfAbsent(h.getTableMap(), t -> new HashSet<>()).addAll(h.getFetchViewColumns());
        }

        for (final Map.Entry<TableMap, Set<Integer>> entry : tableMapIdMap.entrySet()) {
            TableMap tableMap = entry.getKey();

            final Set<String> relevantColumns = tableMapColumnMap.get(tableMap);
            tableMap = new TableMapSupplier(tableMap, Collections.singletonList(t -> t.view(relevantColumns)));

            tableMaps.add(tableMap);
            tableMapIds.add(entry.getValue());
        }
        //endregion
    }

    public PlotInfo getPlotInfo() {
        return figure.getFigure().getPlotInfo();
    }

    public FigureImpl getFigure() {
        return figure;
    }

    public List<Table> getTables() {
        return tables;
    }

    public List<TableMap> getTableMaps() {
        return tableMaps;
    }

    public List<Set<Integer>> getTableIds() {
        return tableIds;
    }

    public List<Set<Integer>> getTableMapIds() {
        return tableMapIds;
    }
}
