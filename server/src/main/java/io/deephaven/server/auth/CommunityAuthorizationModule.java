//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.auth;

import dagger.Binds;
import dagger.Module;

@Module
public interface CommunityAuthorizationModule {
    @Binds
    AuthorizationProvider bindsAuthorizationProvider(CommunityAuthorizationProvider communityAuthorizationProvider);
}
