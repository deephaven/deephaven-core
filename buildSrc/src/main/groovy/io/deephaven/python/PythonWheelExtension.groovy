package io.deephaven.python

import org.gradle.api.Action
import org.gradle.api.file.CopySpec
import org.gradle.api.internal.file.copy.DefaultCopySpec
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.Property
import org.gradle.util.ConfigureUtil

class PythonWheelExtension {
    private Action<? super CopySpec> contents
    private List<String> sourceDirs;

    PythonWheelExtension(ObjectFactory objectFactory) {
        //objectFactory.newInstance(DefaultCopySpec.class)
        contents {
            exclude 'build', 'dist'
        }
        sourceDirs = []
    }

    void contents(Action<? super CopySpec> action) {
        contents = action
    }
    void contents(Closure closure) {
        contents(ConfigureUtil.configureUsing(closure))
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
