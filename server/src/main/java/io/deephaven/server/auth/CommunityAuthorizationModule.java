package io.deephaven.server.auth;

import dagger.Binds;
import dagger.Module;

@Module
public interface CommunityAuthorizationModule {
    @Binds
    AuthorizationProvider bindsAuthorizationProvider(CommunityAuthorizationProvider communityAuthorizationProvider);
}
