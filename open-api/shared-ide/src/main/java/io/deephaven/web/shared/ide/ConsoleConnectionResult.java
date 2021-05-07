package io.deephaven.web.shared.ide;

import java.io.Serializable;

/**
 * Returned from the server upon successfully starting a script session.
 *
 */
public class ConsoleConnectionResult implements Serializable {

    private ScriptHandle handle;
    private String[] tables;
    private String[] widgets;

    public ScriptHandle getHandle() {
        return handle;
    }

    public void setHandle(ScriptHandle handle) {
        this.handle = handle;
    }

    public String[] getTables() {
        return tables;
    }

    public void setTables(String[] tables) {
        this.tables = tables;
    }

    public String[] getWidgets() {
        return widgets;
    }

    public void setWidgets(String[] widgets) {
        this.widgets = widgets;
    }
}
