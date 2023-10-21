package io.deephaven.python

import org.gradle.api.Action
import org.gradle.api.file.CopySpec
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.Property

class PythonWheelExtension {
    private Action<? super CopySpec> contents
    private List<String> sourceDirs;

    PythonWheelExtension(ObjectFactory objectFactory) {
        contents {
            exclude 'build', 'dist'
        }
        sourceDirs = []
    }

    void contents(Action<? super CopySpec> action) {
        contents = action
    }

    Action<? super CopySpec> contents() {
        return contents;
    }

    void src(String srcDir) {
        sourceDirs += srcDir
    }
    List<String> src() {
        return sourceDirs
    }
}
