package io.deephaven.web.client.api;

public class HierarchicalTableTestGwt extends AbstractAsyncGwtTestCase {
    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }

    private final TableSourceBuilder tables = new TableSourceBuilder()
            .script("from deephaven import empty_table, time_table")
            .script("", "");

    public void testRefreshingTreeTable() {

    }
}
