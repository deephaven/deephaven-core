package io.deephaven.server.notebook;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.grpc.BindableService;

/**
 * gRPC Storage Service implementation based on the filesystem.
 */
@Module
public interface FilesystemStorageServiceModule {
    @Binds
    @IntoSet
    BindableService bindNotebookServiceGrpcImpl(FilesystemStorageServiceGrpcImpl instance);
}
