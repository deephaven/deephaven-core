//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.relative;

import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.SerializableSupplier;

import java.util.Map;
import java.util.function.Function;

/**
 * While this class is in the public source set, it is meant to support testing use cases only and should not be used in
 * production.
 *
 * @see <a href="https://github.com/apache/iceberg/issues/1617">Support relative paths in Table Metadata</a>
 */
@VisibleForTesting
public final class RelativeFileIO implements HadoopConfigurable, DelegateFileIO {
    public static final String BASE_PATH = "relative.base-path";
    private static final String IO_DEFAULT_IMPL = ResolvingFileIO.class.getName();

    private String basePath;

    private DelegateFileIO io;

    private SerializableSupplier<Configuration> hadoopConf;

    public RelativeFileIO() {}

    public RelativeFileIO(Configuration hadoopConf) {
        this(new SerializableConfiguration(hadoopConf)::get);
    }

    public RelativeFileIO(SerializableSupplier<Configuration> hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    @Override
    public Configuration getConf() {
        return hadoopConf.get();
    }

    @Override
    public void setConf(Configuration conf) {
        this.hadoopConf = new SerializableConfiguration(conf)::get;
    }

    @Override
    public void serializeConfWith(Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
        this.hadoopConf = confSerializer.apply(getConf());
    }

    public String absoluteLocation(String location) {
        return basePath + location;
    }

    private String relativeLocation(String location) {
        if (!location.startsWith(basePath)) {
            throw new IllegalStateException();
        }
        return location.substring(basePath.length());
    }

    @Override
    public void initialize(Map<String, String> properties) {
        this.basePath = StringUtils.appendIfMissing(properties.get(BASE_PATH), "/");
        // We can add a property here later if we need to override the default
        // final String impl = properties.getOrDefault(IO_IMPL, IO_DEFAULT_IMPL);
        final FileIO fileIO = CatalogUtil.loadFileIO(IO_DEFAULT_IMPL, properties, hadoopConf.get());
        if (!(fileIO instanceof DelegateFileIO)) {
            throw new IllegalArgumentException("filoIO must be DelegateFileIO, " + fileIO.getClass());
        }
        this.io = (DelegateFileIO) fileIO;
    }

    @Override
    public Map<String, String> properties() {
        return io.properties();
    }

    @Override
    public InputFile newInputFile(String path) {
        return new RelativeInputFile(path, io.newInputFile(absoluteLocation(path)));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        return new RelativeInputFile(path, io.newInputFile(absoluteLocation(path), length));
    }

    @Override
    public OutputFile newOutputFile(String path) {
        return new RelativeOutputFile(path, io.newOutputFile(absoluteLocation(path)));
    }

    @Override
    public void deleteFiles(Iterable<String> iterable) throws BulkDeletionFailureException {
        io.deleteFiles(Streams.stream(iterable).map(this::absoluteLocation)::iterator);
    }

    @Override
    public Iterable<FileInfo> listPrefix(String s) {
        return Streams.stream(io.listPrefix(absoluteLocation(s)))
                .map(x -> new FileInfo(relativeLocation(x.location()), x.size(), x.createdAtMillis()))::iterator;
    }

    @Override
    public void deletePrefix(String s) {
        io.deletePrefix(absoluteLocation(s));
    }

    @Override
    public void deleteFile(String path) {
        io.deleteFile(absoluteLocation(path));
    }

    @Override
    public void close() {
        if (io != null) {
            io.close();
        }
    }
}
