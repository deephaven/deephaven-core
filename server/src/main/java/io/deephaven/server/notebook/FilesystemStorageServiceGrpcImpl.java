//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.notebook;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;
import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import io.deephaven.configuration.Configuration;
import io.deephaven.configuration.DataDir;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.CreateDirectoryRequest;
import io.deephaven.proto.backplane.grpc.CreateDirectoryResponse;
import io.deephaven.proto.backplane.grpc.DeleteItemRequest;
import io.deephaven.proto.backplane.grpc.DeleteItemResponse;
import io.deephaven.proto.backplane.grpc.FetchFileRequest;
import io.deephaven.proto.backplane.grpc.FetchFileResponse;
import io.deephaven.proto.backplane.grpc.ItemInfo;
import io.deephaven.proto.backplane.grpc.ItemType;
import io.deephaven.proto.backplane.grpc.ListItemsRequest;
import io.deephaven.proto.backplane.grpc.ListItemsResponse;
import io.deephaven.proto.backplane.grpc.MoveItemRequest;
import io.deephaven.proto.backplane.grpc.MoveItemResponse;
import io.deephaven.proto.backplane.grpc.SaveFileRequest;
import io.deephaven.proto.backplane.grpc.SaveFileResponse;
import io.deephaven.proto.backplane.grpc.StorageServiceGrpc;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionService;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Stream;

import static com.google.common.io.Files.asByteSource;

/**
 * Default implementation for the StorageService gRPC service, which will use the filesystem to store files on behalf of
 * authenticated clients.
 *
 * <p>
 * Current implementation only checks if a user is logged in, and doesn't provide finer grained access controls to
 * files.
 * </p>
 */
@Singleton
public class FilesystemStorageServiceGrpcImpl extends StorageServiceGrpc.StorageServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(FilesystemStorageServiceGrpcImpl.class);

    private static final String STORAGE_PATH = Configuration.getInstance().getStringWithDefault("storage.path",
            DataDir.get().resolve("storage").toString());

    private static final String WEB_LAYOUT_DIRECTORY =
            Configuration.getInstance().getProperty("web.storage.layout.directory");
    private static final String WEB_NOTEBOOK_DIRECTORY =
            Configuration.getInstance().getProperty("web.storage.notebook.directory");
    private static final String[] PRE_CREATE_PATHS = Configuration.getInstance()
            .getStringArrayFromPropertyWithDefault("storage.path.defaults", new String[] {
                    WEB_LAYOUT_DIRECTORY,
                    WEB_NOTEBOOK_DIRECTORY,
            });

    /**
     * Non-cryptographic hash, not resistant to adversarial collisions, but should suffice for quickly checking for
     * edits to files. We're circumventing the "change the seed each startup", as these hashes should be very low risk,
     * and this ensures that clients will have a stable cache across server restarts.
     */
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128(0);

    /**
     * Presently, the Web IDE requires that all paths start with "/". When this is no longer true, remove this constant.
     */
    @Deprecated
    private static final String REQUIRED_PATH_PREFIX = "/";

    private final Path root = Paths.get(STORAGE_PATH).normalize();
    private final SessionService sessionService;
    private final SessionService.ErrorTransformer errorTransformer;

    @Inject
    public FilesystemStorageServiceGrpcImpl(
            final SessionService sessionService,
            final SessionService.ErrorTransformer errorTransformer) {
        this.sessionService = sessionService;
        this.errorTransformer = errorTransformer;
        try {
            Files.createDirectories(root);
            for (String path : PRE_CREATE_PATHS) {
                Files.createDirectories(resolveOrThrow(path));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize storage", e);
        }
    }

    private Path resolveOrThrow(String incomingPath) {
        if (incomingPath.startsWith(File.separator)) {
            incomingPath = incomingPath.substring(1);
        }
        Path resolved = root.resolve(incomingPath).normalize();
        if (resolved.startsWith(root)) {
            return resolved;
        }
        throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Invalid path: " + incomingPath);
    }

    private void requireNotRoot(Path path, String message) {
        if (path.equals(root)) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, message);
        }
    }

    @Override
    public void listItems(
            @NotNull final ListItemsRequest request,
            @NotNull final StreamObserver<ListItemsResponse> responseObserver) {
        sessionService.getCurrentSession();

        ListItemsResponse.Builder builder = ListItemsResponse.newBuilder();
        PathMatcher matcher =
                request.hasFilterGlob() ? createPathFilter(request.getFilterGlob()) : ignore -> true;
        Path dir = resolveOrThrow(request.getPath());
        builder.setCanonicalPath(REQUIRED_PATH_PREFIX + root.relativize(dir));
        try (Stream<Path> list = Files.list(dir)) {
            for (Path p : (Iterable<Path>) list::iterator) {
                if (!matcher.matches(dir.relativize(p))) {
                    continue;
                }
                BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes.class);
                boolean isDirectory = attrs.isDirectory();
                ItemInfo.Builder info = ItemInfo.newBuilder()
                        .setPath(REQUIRED_PATH_PREFIX + root.relativize(p));
                if (isDirectory) {
                    info.setType(ItemType.DIRECTORY);
                } else {
                    info.setSize(attrs.size())
                            .setEtag(hash(p))// Note, there is a potential race here between the size and the hash
                            .setType(ItemType.FILE);
                }
                builder.addItems(info.build());
            }
        } catch (NoSuchFileException noSuchFileException) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "Directory does not exist");
        } catch (IOException ioe) {
            throw errorTransformer.transform(ioe);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    private static PathMatcher createPathFilter(String filterGlob) {
        if (filterGlob.contains("**")) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Bad glob, only single `*`s are supported");
        }
        if (filterGlob.contains(File.separator)) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "Bad glob, only the same directory can be checked");
        }
        try {
            return FileSystems.getDefault().getPathMatcher("glob:" + filterGlob);
        } catch (PatternSyntaxException e) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "Bad glob, can't parse expression: " + e.getMessage());
        }
    }


    /**
     * Using the default hash function, read the file at the given path and return a hex string of its hash.
     */
    private static String hash(Path path) throws IOException {
        return asByteSource(path.toFile()).hash(HASH_FUNCTION).toString();
    }

    @Override
    public void fetchFile(
            @NotNull final FetchFileRequest request,
            @NotNull final StreamObserver<FetchFileResponse> responseObserver) {
        sessionService.getCurrentSession();

        final byte[] bytes;
        final String etag;
        try {
            bytes = Files.readAllBytes(resolveOrThrow(request.getPath()));
            // Hash those bytes, as long as we are reading them to send, since we want the hash to be consistent
            // with the contents we send. This avoids a race condition, at the cost of requiring that the server
            // always read the full bytes
            etag = ByteSource.wrap(bytes).hash(HASH_FUNCTION).toString();
        } catch (NoSuchFileException noSuchFileException) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "File does not exist");
        } catch (IOException ioe) {
            throw errorTransformer.transform(ioe);
        }

        final FetchFileResponse.Builder response = FetchFileResponse.newBuilder();
        response.setEtag(etag);
        if (!request.hasEtag() || !etag.equals(request.getEtag())) {
            response.setContents(ByteString.copyFrom(bytes));
        }
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void saveFile(
            @NotNull final SaveFileRequest request,
            @NotNull final StreamObserver<SaveFileResponse> responseObserver) {
        sessionService.getCurrentSession();

        Path path = resolveOrThrow(request.getPath());
        requireNotRoot(path, "Can't overwrite the root directory");
        StandardOpenOption option =
                request.getAllowOverwrite() ? StandardOpenOption.TRUNCATE_EXISTING : StandardOpenOption.CREATE_NEW;

        String etag;
        byte[] bytes = request.getContents().toByteArray();
        try {
            etag = ByteSource.wrap(bytes).hash(HASH_FUNCTION).toString();
            Files.write(path, bytes, StandardOpenOption.CREATE, option);
        } catch (FileAlreadyExistsException alreadyExistsException) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "File already exists");
        } catch (NoSuchFileException noSuchFileException) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "Directory does not exist");
        } catch (IOException ioe) {
            throw errorTransformer.transform(ioe);
        }

        responseObserver.onNext(SaveFileResponse.newBuilder().setEtag(etag).build());
        responseObserver.onCompleted();
    }

    @Override
    public void moveItem(
            @NotNull final MoveItemRequest request,
            @NotNull final StreamObserver<MoveItemResponse> responseObserver) {
        sessionService.getCurrentSession();

        Path source = resolveOrThrow(request.getOldPath());
        Path target = resolveOrThrow(request.getNewPath());
        requireNotRoot(target, "Can't overwrite the root directory");

        StandardCopyOption[] options =
                request.getAllowOverwrite() ? new StandardCopyOption[] {StandardCopyOption.REPLACE_EXISTING}
                        : new StandardCopyOption[0];

        try {
            Files.move(source, target, options);
        } catch (NoSuchFileException noSuchFileException) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "File does not exist, cannot rename");
        } catch (FileAlreadyExistsException alreadyExistsException) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "File already exists, cannot rename to replace");
        } catch (DirectoryNotEmptyException directoryNotEmptyException) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "Cannot replace non-empty directory");
        } catch (IOException ioe) {
            throw errorTransformer.transform(ioe);
        }
        responseObserver.onNext(MoveItemResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void createDirectory(
            @NotNull final CreateDirectoryRequest request,
            @NotNull final StreamObserver<CreateDirectoryResponse> responseObserver) {
        sessionService.getCurrentSession();

        Path dir = resolveOrThrow(request.getPath());
        requireNotRoot(dir, "Can't overwrite the root directory");

        try {
            Files.createDirectory(dir);
        } catch (FileAlreadyExistsException fileAlreadyExistsException) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Something already exists with that name");
        } catch (NoSuchFileException noSuchFileException) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Can't create directory, parent directory doesn't exist");
        } catch (IOException ioe) {
            throw errorTransformer.transform(ioe);
        }
        responseObserver.onNext(CreateDirectoryResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteItem(
            @NotNull final DeleteItemRequest request,
            @NotNull final StreamObserver<DeleteItemResponse> responseObserver) {
        sessionService.getCurrentSession();

        Path path = resolveOrThrow(request.getPath());
        requireNotRoot(path, "Can't delete the root directory");

        try {
            Files.delete(path);
        } catch (NoSuchFileException noSuchFileException) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "Cannot delete, file does not exists");
        } catch (DirectoryNotEmptyException directoryNotEmptyException) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "Cannot delete non-empty directory");
        } catch (IOException ioe) {
            throw errorTransformer.transform(ioe);
        }
        responseObserver.onNext(DeleteItemResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
