package io.deephaven.server.notebook;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;
import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import io.deephaven.configuration.Configuration;
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
import io.deephaven.server.session.SessionService;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.regex.PatternSyntaxException;

import static com.google.common.io.Files.asByteSource;

/**
 * Default implementation for the StorageService gRPC service, which will use the filesystem to store files on behalf of
 * authenticated clients.
 *
 * Current implementation only checks if a user is logged in, and doesn't provide finer grained access controls to
 * files.
 */
@Singleton
public class FilesystemStorageServiceGrpcImpl extends StorageServiceGrpc.StorageServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(FilesystemStorageServiceGrpcImpl.class);

    private static final String STORAGE_PATH =
            Configuration.getInstance().getStringWithDefault("storage.path", "<workspace>/storage")
                    .replace("<workspace>", Configuration.getInstance().getWorkspacePath());

    /**
     * Non-cryptographic hash, not resistant to adversarial collisions, but should suffice for quickly checking for
     * edits to files. By design, hashes might not be stable from one startup to the next, but will at least efficiently
     * guard against client's needing to load the same file again rapidly.
     */
    private static final HashFunction HASH_FUNCTION = Hashing.goodFastHash(128);

    private final Path root = Paths.get(STORAGE_PATH).normalize();
    private final SessionService sessionService;

    @Inject
    public FilesystemStorageServiceGrpcImpl(SessionService sessionService) {
        this.sessionService = sessionService;
        try {
            Files.createDirectories(root);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize storage", e);
        }
    }

    private Path resolveOrThrow(String relativePath) {
        Path resolved = root.resolve(relativePath).normalize();
        if (resolved.startsWith(root)) {
            return resolved;
        }
        throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Invalid path: " + relativePath);
    }

    @Override
    public void listItems(ListItemsRequest request, StreamObserver<ListItemsResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            sessionService.getCurrentSession();

            ListItemsResponse.Builder builder = ListItemsResponse.newBuilder();
            DirectoryStream.Filter<Path> matcher =
                    request.hasFilterGlob() ? checkMatcher(request.getFilterGlob()) : ignore -> true;
            Path dir = resolveOrThrow(request.getPath());
            try (DirectoryStream<Path> list = Files.newDirectoryStream(dir, matcher)) {
                for (Path p : list) {
                    BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes.class);
                    boolean isDirectory = attrs.isDirectory();
                    ItemInfo.Builder info = ItemInfo.newBuilder().setPath(p.getFileName().toString());
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
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Directory does not exist");
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        });
    }

    private static DirectoryStream.Filter<Path> checkMatcher(String filterGlob) {
        if (filterGlob.contains("**")) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Bad glob, only single `*`s are supported");
        }
        if (filterGlob.contains("/")) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "Bad glob, only the same directory can be checked");
        }
        try {
            return FileSystems.getDefault().getPathMatcher("glob:" + filterGlob)::matches;
        } catch (PatternSyntaxException e) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
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
    public void fetchFile(FetchFileRequest request, StreamObserver<FetchFileResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
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
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "File does not exist");
            }
            final FetchFileResponse.Builder response = FetchFileResponse.newBuilder();
            response.setEtag(etag);
            if (request.hasEtag() && !etag.equals(request.getEtag())) {
                response.setContents(ByteString.copyFrom(bytes));
            }
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void saveFile(SaveFileRequest request, StreamObserver<SaveFileResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            sessionService.getCurrentSession();

            Path path = resolveOrThrow(request.getPath());
            StandardOpenOption option =
                    request.getNewFile() ? StandardOpenOption.CREATE_NEW : StandardOpenOption.CREATE;
            try {
                Files.write(path, request.getContents().toByteArray(), option);
            } catch (FileAlreadyExistsException alreadyExistsException) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "File already exists");
            } catch (NoSuchFileException noSuchFileException) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Directory does not exist");
            }
            responseObserver.onNext(SaveFileResponse.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void moveItem(MoveItemRequest request, StreamObserver<MoveItemResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            sessionService.getCurrentSession();

            Path source = resolveOrThrow(request.getOldPath());
            Path target = resolveOrThrow(request.getNewPath());

            try {
                Files.move(source, target);
            } catch (NoSuchFileException noSuchFileException) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "File does not exist, cannot rename");
            } catch (FileAlreadyExistsException alreadyExistsException) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                        "File already exists, cannot rename to replace");
            }
            responseObserver.onNext(MoveItemResponse.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void createDirectory(CreateDirectoryRequest request,
            StreamObserver<CreateDirectoryResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            sessionService.getCurrentSession();

            Path dir = resolveOrThrow(request.getPath());
            try {
                Files.createDirectory(dir);
            } catch (FileAlreadyExistsException fileAlreadyExistsException) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                        "Something already exists with that name");
            } catch (NoSuchFileException noSuchFileException) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                        "Can't create directory, parent directory doesn't exist");
            }
            responseObserver.onNext(CreateDirectoryResponse.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void deleteItem(DeleteItemRequest request, StreamObserver<DeleteItemResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            sessionService.getCurrentSession();

            Path path = resolveOrThrow(request.getPath());
            try {
                Files.delete(path);
            } catch (NoSuchFileException noSuchFileException) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Cannot delete, file does not exists");
            } catch (DirectoryNotEmptyException directoryNotEmptyException) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Cannot delete non-empty directory");
            }
            responseObserver.onNext(DeleteItemResponse.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }
}
