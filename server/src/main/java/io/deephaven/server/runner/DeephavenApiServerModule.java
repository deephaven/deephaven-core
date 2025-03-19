//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import dagger.Module;
import io.deephaven.server.appmode.AppModeModule;
import io.deephaven.server.appmode.ApplicationsModule;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.arrow.ExchangeMarshallerModule;
import io.deephaven.server.auth.AuthContextModule;
import io.deephaven.server.config.ConfigServiceModule;
import io.deephaven.server.console.ConsoleModule;
import io.deephaven.server.grpc.GrpcModule;
import io.deephaven.server.hierarchicaltable.HierarchicalTableServiceModule;
import io.deephaven.server.notebook.FilesystemStorageServiceModule;
import io.deephaven.server.object.ObjectServiceModule;
import io.deephaven.server.partitionedtable.PartitionedTableServiceModule;
import io.deephaven.server.plugin.PluginsModule;
import io.deephaven.server.runner.scheduler.SchedulerModule;
import io.deephaven.server.runner.updategraph.UpdateGraphModule;
import io.deephaven.server.session.ScriptSessionModule;
import io.deephaven.server.session.SessionModule;
import io.deephaven.server.table.TableModule;
import io.deephaven.server.table.inputtables.InputTableModule;
import io.deephaven.server.uri.UriModule;

@Module(includes = {
        AppModeModule.class,
        ApplicationsModule.class,
        ArrowModule.class,
        AuthContextModule.class,
        UriModule.class,
        SessionModule.class,
        TableModule.class,
        InputTableModule.class,
        ConsoleModule.class,
        ObjectServiceModule.class,
        PluginsModule.class,
        PartitionedTableServiceModule.class,
        HierarchicalTableServiceModule.class,
        FilesystemStorageServiceModule.class,
        ConfigServiceModule.class,
        SchedulerModule.class,
        UpdateGraphModule.class,
        GrpcModule.class,
        ScriptSessionModule.class,
        ExchangeMarshallerModule.class
})
public class DeephavenApiServerModule {

}
