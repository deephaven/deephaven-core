package io.deephaven.tools.docker

import org.gradle.nativeplatform.platform.internal.DefaultNativePlatform

enum Architecture {
    AMD64("amd64"),
    ARM64("arm64");

    static Architecture fromHost() {
        String archName = DefaultNativePlatform.host().getArchitecture().getName()
        switch (archName) {
            case "x86-64":
                return AMD64
            case "arm-v8":
                return ARM64
            default:
                throw new IllegalStateException("Unable to determine proper docker architecture for " + archName)
        }
    }

    private final String dockerName

    Architecture(String dockerName) {
        this.dockerName = dockerName
    }

    @Override
    String toString() {
        dockerName
    }
}