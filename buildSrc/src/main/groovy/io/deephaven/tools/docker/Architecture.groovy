package io.deephaven.tools.docker

import org.gradle.api.Project
import org.gradle.nativeplatform.platform.internal.DefaultNativePlatform

enum Architecture {
    AMD64("amd64"),
    ARM64("arm64");

    static Architecture targetArchitecture(Project project) {
        if (project.hasProperty('docker.targetArch')) {
            return fromDockerName(project.property('docker.targetArch') as String)
        } else {
            return fromHost()
        }
    }

    static Architecture fromHost() {
        String archName = DefaultNativePlatform.host().getArchitecture().getName()
        // See org.gradle.nativeplatform.platform.internal.Architectures for constants
        switch (archName) {
            case "x86-64":
            case "x86_64":
            case "amd64":
            case "x64":
                return AMD64
            case "aarch64":
            case "arm-v8":
            case "arm64":
                return ARM64
            default:
                throw new IllegalStateException("Unable to determine proper docker architecture for " + archName)
        }
    }

    static Architecture fromDockerName(String dockerName) {
        values().find { a -> a.dockerName == dockerName }
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
