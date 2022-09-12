package io.deephaven.server.notebook;

import dagger.Binds;
import dagger.Module;
import dagger.multibindings.IntoSet;
import io.grpc.BindableService;

@Module
public interface NotebookServiceModule {
    @Binds
    @IntoSet
    BindableService bindNotebookServiceGrpcImpl(NotebookServiceGrpcImpl instance);
}
