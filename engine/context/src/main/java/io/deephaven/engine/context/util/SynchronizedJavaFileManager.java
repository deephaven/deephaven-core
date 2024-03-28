//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context.util;

import javax.tools.FileObject;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import java.io.IOException;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.Set;

public class SynchronizedJavaFileManager implements JavaFileManager {

    private final JavaFileManager delegate;

    public SynchronizedJavaFileManager(JavaFileManager delegate) {
        this.delegate = delegate;
    }

    @Override
    public synchronized ClassLoader getClassLoader(Location location) {
        return delegate.getClassLoader(location);
    }

    @Override
    public synchronized Iterable<JavaFileObject> list(
            Location location,
            String packageName,
            Set<JavaFileObject.Kind> kinds,
            boolean recurse) throws IOException {
        return delegate.list(location, packageName, kinds, recurse);
    }

    @Override
    public synchronized String inferBinaryName(Location location, JavaFileObject file) {
        return delegate.inferBinaryName(location, file);
    }

    @Override
    public synchronized boolean isSameFile(FileObject a, FileObject b) {
        return delegate.isSameFile(a, b);
    }

    @Override
    public synchronized boolean handleOption(String current, Iterator<String> remaining) {
        return delegate.handleOption(current, remaining);
    }

    @Override
    public synchronized boolean hasLocation(Location location) {
        return delegate.hasLocation(location);
    }

    @Override
    public synchronized JavaFileObject getJavaFileForInput(
            Location location,
            String className,
            JavaFileObject.Kind kind) throws IOException {
        return delegate.getJavaFileForInput(location, className, kind);
    }

    @Override
    public synchronized JavaFileObject getJavaFileForOutput(
            Location location,
            String className,
            JavaFileObject.Kind kind,
            FileObject sibling) throws IOException {
        return delegate.getJavaFileForOutput(location, className, kind, sibling);
    }

    @Override
    public synchronized FileObject getFileForInput(
            Location location,
            String packageName,
            String relativeName) throws IOException {
        return delegate.getFileForInput(location, packageName, relativeName);
    }

    @Override
    public synchronized FileObject getFileForOutput(
            Location location,
            String packageName,
            String relativeName,
            FileObject sibling) throws IOException {
        return delegate.getFileForOutput(location, packageName, relativeName, sibling);
    }

    @Override
    public synchronized void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        delegate.close();
    }

    @Override
    public synchronized int isSupportedOption(String option) {
        return delegate.isSupportedOption(option);
    }

    @Override
    public synchronized Location getLocationForModule(Location location, String moduleName) throws IOException {
        return delegate.getLocationForModule(location, moduleName);
    }

    @Override
    public synchronized Location getLocationForModule(Location location, JavaFileObject fo) throws IOException {
        return delegate.getLocationForModule(location, fo);
    }

    @Override
    public synchronized <S> ServiceLoader<S> getServiceLoader(Location location, Class<S> service) throws IOException {
        return delegate.getServiceLoader(location, service);
    }

    @Override
    public synchronized String inferModuleName(Location location) throws IOException {
        return delegate.inferModuleName(location);
    }

    @Override
    public synchronized Iterable<Set<Location>> listLocationsForModules(Location location) throws IOException {
        return delegate.listLocationsForModules(location);
    }

    @Override
    public synchronized boolean contains(Location location, FileObject fo) throws IOException {
        return delegate.contains(location, fo);
    }
}
