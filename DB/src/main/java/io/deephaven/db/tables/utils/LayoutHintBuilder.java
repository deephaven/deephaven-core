package io.deephaven.db.tables.utils;

import io.deephaven.base.StringUtils;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;

/**
 * The builder class for use in assembling layout hints suitable for use with {@link io.deephaven.db.tables.Table#layoutHints(LayoutHintBuilder)} or
 * {@link io.deephaven.db.tables.Table#layoutHints(String)}.
 */
@ScriptApi
public class LayoutHintBuilder {
    private static final String AFD_FETCH_PARAM = "fetch";
    private static final int UNDEFINED_FETCH_SIZE = -1;

    private boolean savedLayoutsAllowed = true;
    private Set<String> frontCols;
    private Set<String> backCols;
    private Map<String, AutoFilterData> autoFilterCols;
    private Set<String> hiddenCols;
    private Set<String> freezeCols;
    private Set<String> alwaysSubscribedCols;
    private Set<String> groupableColumns;

    /**
     * Helper class to maintain sub-properties for auto filter columns
     */
    private static class AutoFilterData {
        final String column;
        int fetchSize;

        AutoFilterData(String column) {
            this(column, UNDEFINED_FETCH_SIZE);
        }

        AutoFilterData(String column, int fetchSize) {
            this.column = column;
            this.fetchSize = fetchSize;
        }

        /**
         * Serialize this object to a string suitable for inclusion in the builder's
         * parameter string.
         *
         * @return a string of the format column(:param&value)+
         */
        @NotNull
        String forBuilder() {
            if(fetchSize > 0) {
                return column + ":" + AFD_FETCH_PARAM + "&" + fetchSize;
            }

            return column;
        }

        /**
         * Convert a string of the format defined by {@link #forBuilder()} into a proper AutoFilterData object
         *
         * @param string the string to parse
         * @return an AutoFilterData instance
         */
        @NotNull
        static AutoFilterData fromString(String string) {
            final String[] parts = string.split(":");
            if(parts.length == 0) {
                throw new IllegalArgumentException("Improperly formatted AutoFilterData string: " + string);
            }

            final String column = parts[0];
            try {
                NameValidator.validateColumnName(column);
            } catch(NameValidator.InvalidNameException ex) {
                throw new IllegalArgumentException("AutoFilterData invalid column name", ex);
            }

            int localFetchSize = UNDEFINED_FETCH_SIZE;
            if(parts.length > 1) {
                for(int i = 1; i < parts.length; i++) {
                    String[] paramParts = parts[i].split("&");
                    if(paramParts.length != 2) {
                        throw new IllegalArgumentException("Only one value permitted in AutoFilterData parameter string; instead there are: " + parts.length + " in " + parts[i]);
                    }

                    //noinspection SwitchStatementWithTooFewBranches
                    switch(paramParts[0]) {
                        case AFD_FETCH_PARAM:
                            try {
                                localFetchSize = Integer.parseInt(paramParts[1]);
                            } catch (NumberFormatException ex) {
                                throw new IllegalArgumentException("Invalid value for AutoFilterData fetch size parameter: " + paramParts[1]);
                            }
                            break;
                    }
                }
            }

            return new AutoFilterData(column,localFetchSize);
        }
    }

    private LayoutHintBuilder() {}

    //region Builder Methods
    /**
     * Create a LayoutHintBuilder from the specified parameter string.
     *
     * @param attrs the parameter string
     * @return a LayoutHintBuilder for the input parameter string
     */
    @ScriptApi
    @NotNull
    public static LayoutHintBuilder fromString(String attrs) {
        final Map<String, String> options = Arrays.stream(attrs.split(";"))
                .map(attr -> attr.split("="))
                .collect(Collectors.toMap(parts -> parts[0], parts -> parts.length == 2 ? parts[1] : ""));

        final LayoutHintBuilder lhb = new LayoutHintBuilder();
        if(options.containsKey("noSavedLayouts")) {
            lhb.savedLayouts(false);
        }

        final String frontStr = options.get("front");
        if(frontStr != null && !frontStr.isEmpty()) {
            lhb.atFront(frontStr.split(","));
        }

        final String endStr = options.get("back");
        if(endStr != null && !endStr.isEmpty()) {
            lhb.atEnd(endStr.split(","));
        }

        final String hideStr = options.get("hide");
        if(hideStr != null && !hideStr.isEmpty()) {
            lhb.hide(hideStr.split(","));
        }

        final String autoStr = options.get("autofilter");
        if(!io.deephaven.db.util.string.StringUtils.isNullOrEmpty(autoStr)) {
            final String[] filters = autoStr.split(",");
            Arrays.stream(filters)
                    .map(AutoFilterData::fromString)
                    .forEach(lhb::addAutofilterData);
        }

        final String freezeStr = options.get("freeze");
        if(freezeStr != null && !freezeStr.isEmpty()) {
            lhb.freeze(freezeStr.split(","));
        }

        final String subscribedStr = options.get("subscribed");
        if(subscribedStr != null && !subscribedStr.isEmpty()) {
            final String[] subscribedColumns = subscribedStr.split(",");
            lhb.alwaysSubscribed(subscribedColumns);
        }

        final String groupableStr = options.get("groupable");
        if(groupableStr != null && !groupableStr.isEmpty()) {
            final String[] groupableColumns = groupableStr.split(",");
            lhb.groupableColumns(groupableColumns);
        }

        return lhb;
    }

    /**
     * Create a new LayoutHintBuilder.
     *
     * @return a new LayoutHintBuilder
     */
    @ScriptApi
    @NotNull
    public static LayoutHintBuilder get() {
        return new LayoutHintBuilder();
    }

    /**
     * @see LayoutHintBuilder#atFront(Collection)
     */
    @ScriptApi
    public LayoutHintBuilder atFront(String... cols) {
        return atFront(cols == null ? null : Arrays.asList(cols));
    }

    /**
     * Indicate the specified columns should appear as the first N columns of the table when displayed.
     *
     * @param cols the columns to show at front
     * @return this LayoutHintBuilder
     */
    @ScriptApi
    public LayoutHintBuilder atFront(Collection<String> cols) {
        if(cols == null || cols.isEmpty()) {
            frontCols = null;
            return this;
        }

        if(frontCols == null) {
            frontCols = new LinkedHashSet<>(cols.size());
        }

        frontCols.addAll(cols);

        if(backCols != null) {
            backCols.removeAll(frontCols);
        }

        return this;
    }

    /**
     * @see LayoutHintBuilder#atEnd(Collection)
     */
    @ScriptApi
    public LayoutHintBuilder atEnd(String... cols) {
        return atEnd(cols == null ? null : Arrays.asList(cols));
    }

    /**
     * Indicate the specified columns should appear as the last N columns of the table when displayed.
     *
     * @param cols the columns to show at the back
     * @return this LayoutHintBuilder
     */
    @ScriptApi
    public LayoutHintBuilder atEnd(Collection<String> cols) {
        if(cols == null || cols.isEmpty()) {
            backCols = null;
            return this;
        }

        if(backCols == null) {
            backCols = new LinkedHashSet<>(cols.size());
        }

        backCols.addAll(cols);

        if(frontCols != null) {
            frontCols.removeAll(backCols);
        }

        return this;
    }

    /**
     * @see LayoutHintBuilder#hide(Collection)
     */
    @ScriptApi
    public LayoutHintBuilder hide(String... cols) {
        return hide(cols == null ? null : Arrays.asList(cols));
    }

    /**
     * Indicate the specified columns should be hidden by default.
     *
     * @param cols the columns to initially hide
     * @return this LayoutHintBuilder
     */
    @ScriptApi
    public LayoutHintBuilder hide(Collection<String> cols) {
        if(cols == null || cols.isEmpty()) {
            hiddenCols = null;
            return this;
        }

        if(hiddenCols == null) {
            hiddenCols = new HashSet<>(cols.size());
        }

        hiddenCols.addAll(cols);

        return this;
    }

    /**
     * @see LayoutHintBuilder#autoFilter(Collection)
     */
    @ScriptApi
    public LayoutHintBuilder autoFilter(String... cols) {
        return autoFilter(cols == null ? null : Arrays.asList(cols));
    }

    /**
     * Indicate the specified columns should be configured as AutoFilter columns
     *
     * @param cols the columns to enable as AutoFilter columns
     * @return this LayoutHintBuilder
     */
    @ScriptApi
    public LayoutHintBuilder autoFilter(Collection<String> cols) {
        if(cols == null || cols.isEmpty()) {
            autoFilterCols = null;
            return this;
        }

        if(autoFilterCols == null) {
            autoFilterCols = new HashMap<>(cols.size());
        }

        cols.stream()
                .map(AutoFilterData::new)
                .forEach(c -> autoFilterCols.put(c.column, c));

        return this;
    }

    private void addAutofilterData(@NotNull AutoFilterData afd) {
        if(autoFilterCols == null) {
            autoFilterCols = new HashMap<>();
        }

        autoFilterCols.put(afd.column,afd);
    }

    /**
     * Set the default initial number of rows to fetch for columns that have been marked as {@link LayoutHintBuilder#autoFilter(Collection) AutoFilter}
     * columns.
     *
     * @param col the column to set the fetch size for
     * @param size the number of rows to fetch initially
     * @return this LayoutHintBuilder
     */
    @ScriptApi
    public LayoutHintBuilder autoFilterFetchSize(String col, int size) {
        if(autoFilterCols == null) {
            throw new IllegalStateException("Autofilter is not enabled for any columns");
        }

        final AutoFilterData afd = autoFilterCols.get(col);
        if(afd == null) {
            throw new IllegalArgumentException("Autofilter not enabled for column " + col);
        }

        if(size <= 0) {
            throw new IllegalArgumentException("Illegal Autofilter fetch size: " + size);
        }

        afd.fetchSize = size;

        return this;
    }

    /**
     * @see LayoutHintBuilder#freeze(Collection)
     */
    @ScriptApi
    public LayoutHintBuilder freeze(String... cols) {
        return freeze(cols == null ? null : Arrays.asList(cols));
    }

    /**
     * Indicate the specified columns should be frozen (displayed as the first N, unmovable columns) upon display.
     *
     * @param cols the columns to freeze
     * @return this LayoutHintBuilder
     */
    @ScriptApi
    public LayoutHintBuilder freeze(Collection<String> cols) {
        if(cols == null || cols.isEmpty()) {
            freezeCols = null;
            return this;
        }

        if(freezeCols == null) {
            freezeCols = new LinkedHashSet<>(cols.size());
        }

        freezeCols.addAll(cols);

        return this;
    }

    /**
     * Indicate that the UI should maintain a subscription to the specified columns within viewports,
     * even if they are out of view.
     *
     * @param columns the columns to keep subscribed
     * @return this LayoutHintBuilder
     */
    @ScriptApi
    public LayoutHintBuilder alwaysSubscribed(String... columns) {
        if(alwaysSubscribedCols == null) {
            alwaysSubscribedCols = new HashSet<>();
        }

        if(columns != null && columns.length > 0) {
            alwaysSubscribedCols.clear();
            alwaysSubscribedCols.addAll(Arrays.asList(columns));
        }

        return this;
    }

    /**
     * Enable or disable saved layouts for the specified table.
     *
     * @param allowed if layout saving is enabled
     * @return this LayoutHintBuilder
     */
    @ScriptApi
    public LayoutHintBuilder savedLayouts(boolean allowed) {
        savedLayoutsAllowed = allowed;
        return this;
    }

    /**
     * Set the columns which are allowed to be used as UI-driven rollup columns.
     *
     * @param columns the columns.
     * @return this LayoutHintBuilder
     */
    @ScriptApi
    public LayoutHintBuilder groupableColumns(String... columns) {
        return groupableColumns(Arrays.asList(columns));
    }

    /**
     * Set the columns which are allowed to be used as UI-driven rollup columns.
     *
     * @param columns the columns.
     * @return this LayoutHintBuilder
     */
    @ScriptApi
    public LayoutHintBuilder groupableColumns(Collection<String> columns) {
        groupableColumns = new HashSet<>(columns);
        return this;
    }

    //endregion

    /**
     * Create an appropriate parameter string suitable for use with {@link io.deephaven.db.tables.Table#layoutHints(String)}.
     *
     * @return this LayoutHintBuilder as a string
     */
    @ScriptApi
    public String build() {
        final StringBuilder sb = new StringBuilder();

        if(!savedLayoutsAllowed) {
            sb.append("noSavedLayouts;");
        }

        if(frontCols != null && !frontCols.isEmpty()) {
            sb.append("front=").append(StringUtils.joinStrings(frontCols,",")).append(';');
        }

        if(backCols != null && !backCols.isEmpty()) {
            sb.append("back=").append(StringUtils.joinStrings(backCols,",")).append(';');
        }

        if(hiddenCols != null && !hiddenCols.isEmpty()) {
            sb.append("hide=").append(StringUtils.joinStrings(hiddenCols,",")).append(';');
        }

        if(autoFilterCols != null && !autoFilterCols.isEmpty()) {
            sb.append("autofilter=").append(StringUtils.joinStrings(autoFilterCols.values().stream().map(AutoFilterData::forBuilder),",")).append(';');
        }

        if(freezeCols != null && !freezeCols.isEmpty()) {
            sb.append("freeze=").append(StringUtils.joinStrings(freezeCols,",")).append(';');
        }

        if(alwaysSubscribedCols != null && !alwaysSubscribedCols.isEmpty()) {
            sb.append("subscribed=").append(String.join(",", alwaysSubscribedCols)).append(';');
        }

        if(groupableColumns != null && !groupableColumns.isEmpty()) {
            sb.append("groupable=").append(String.join(",", groupableColumns)).append(';');
        }

        return sb.toString();
    }

    //region Getters
    /**
     * Check if saved layouts should be allowed.
     *
     * @return if saved layouts are allowed
     */
    public boolean areSavedLayoutsAllowed() {
        return savedLayoutsAllowed;
    }

    /**
     * Get the ordered set of columns that should be displayed up front.
     *
     * @return an ordered set of columns to display up as the first N columns
     */
    public @NotNull Set<String> getFrontCols() {
        return frontCols == null ? Collections.emptySet() : Collections.unmodifiableSet(frontCols);
    }

    /**
     * Get the ordered set of columns that should be displayed as the last N columns.
     *
     * @return an ordfered set of columns to display at the end.
     */
    public @NotNull Set<String> getBackCols() {
        return backCols == null ? Collections.emptySet() : Collections.unmodifiableSet(backCols);
    }

    /**
     * Get the set of columns that should be hidden by default.
     *
     * @return the set of columns that should be hidden
     */
    public @NotNull Set<String> getHiddenCols() {
        return hiddenCols == null ? Collections.emptySet() : Collections.unmodifiableSet(hiddenCols);
    }

    /**
     * Get the set of columns that should be enabled for AutoFilter.
     *
     * @return the set of columns enabled for AutoFilter
     */
    public @NotNull Set<String> getAutoFilterCols() {
        return autoFilterCols == null ? Collections.emptySet() : Collections.unmodifiableSet(autoFilterCols.keySet());
    }

    /**
     * Get the number of rows to fetch in the initial AutoFilter data fetch.
     *
     * @param column the column to set the fetch size for
     * @return the number of rows to fetch initially
     */
    public int getAutoFilterFetchSize(String column) {
        if(autoFilterCols == null) {
            return UNDEFINED_FETCH_SIZE;
        }

        final AutoFilterData afd = autoFilterCols.get(column);
        return afd == null ? UNDEFINED_FETCH_SIZE : afd.fetchSize;
    }

    /**
     * Get the ordered set of columns that should be frozen.
     *
     * @return the ordered set of columns that should be frozen
     */
    public @NotNull Set<String> getFreezeCols() {
        return freezeCols == null ? Collections.emptySet() : Collections.unmodifiableSet(freezeCols);
    }

    /**
     * Get the set of columns that should always remain subscribed.
     *
     * @return the set of columns to be subscribed.
     */
    public @NotNull Set<String> getAlwaysSubscribedCols() {
        return alwaysSubscribedCols == null ? Collections.emptySet() : Collections.unmodifiableSet(alwaysSubscribedCols);
    }

    /**
     * Get the set of columns allowed to participate in UI-driven rollups.
     *
     * @return the set of columns
     */
    public @NotNull Set<String> getGroupableColumns() {
        return groupableColumns == null ? Collections.emptySet() : Collections.unmodifiableSet(groupableColumns);
    }
    //endregion
}
