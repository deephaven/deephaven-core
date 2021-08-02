package io.deephaven.client;

import dagger.Module;

@Module(
    includes = {SessionServiceModule.class, TableServiceModule.class, ConsoleServiceModule.class})
public interface ServicesModule {

}
