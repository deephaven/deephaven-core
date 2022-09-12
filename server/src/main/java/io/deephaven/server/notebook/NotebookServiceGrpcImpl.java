package io.deephaven.server.notebook;

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
import io.deephaven.proto.backplane.grpc.FileInfo;
import io.deephaven.proto.backplane.grpc.FileKind;
import io.deephaven.proto.backplane.grpc.ListItemsRequest;
import io.deephaven.proto.backplane.grpc.ListItemsResponse;
import io.deephaven.proto.backplane.grpc.MoveItemRequest;
import io.deephaven.proto.backplane.grpc.MoveItemResponse;
import io.deephaven.proto.backplane.grpc.NotebookServiceGrpc;
import io.deephaven.proto.backplane.grpc.SaveFileRequest;
import io.deephaven.proto.backplane.grpc.SaveFileResponse;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.stream.Stream;

public class NotebookServiceGrpcImpl extends NotebookServiceGrpc.NotebookServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(NotebookServiceGrpcImpl.class);

    private static final String NOTEBOOK_PATH = Configuration.getInstance().getStringWithDefault("notebook.path", "<workspace>/notebooks")
            .replace("<workspace>", Configuration.getInstance().getWorkspacePath());

    private final Path root = Paths.get(NOTEBOOK_PATH).normalize();

    @Inject
    public NotebookServiceGrpcImpl() {
        try {
            Files.createDirectories(root);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to initialize notebooks", e);
        }
    }

    private Optional<Path> resolve(String relativePath) {
        Path resolved = root.resolve(relativePath).normalize();
        if (resolved.startsWith(root)) {
            return Optional.of(resolved);
        }
        return Optional.empty();
    }
    private Path resolveOrThrow(String relativePath) {
        return resolve(relativePath).orElseThrow(() -> GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Invalid path: " + relativePath));
    }
    @Override
    public void listItems(ListItemsRequest request, StreamObserver<ListItemsResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            ListItemsResponse.Builder builder = ListItemsResponse.newBuilder();
            try (Stream<Path> list = Files.list(resolveOrThrow(request.getPath()))) {
                for (Path p : (Iterable<Path>) list::iterator) {
                    builder.addItems(FileInfo.newBuilder()
                            .setPath(p.getFileName().toString())
                            .setSize(Files.isDirectory(p) ? 0 : Files.size(p))
                            .setKind(Files.isDirectory(p) ? FileKind.DIRECTORY : FileKind.FILE)
                            .build());
                }
            } catch (NoSuchFileException noSuchFileException) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Directory does not exist");
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void fetchFile(FetchFileRequest request, StreamObserver<FetchFileResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final byte[] bytes;
            try {
                bytes = Files.readAllBytes(resolveOrThrow(request.getPath()));
            } catch (NoSuchFileException noSuchFileException) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "File does not exist");
            }
            FetchFileResponse.Builder contents = FetchFileResponse.newBuilder().setContents(ByteString.copyFrom(bytes));
            responseObserver.onNext(contents.build());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void saveFile(SaveFileRequest request, StreamObserver<SaveFileResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            Path path = resolveOrThrow(request.getPath());
            StandardOpenOption option = request.getNewFile() ? StandardOpenOption.CREATE_NEW : StandardOpenOption.CREATE;
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
            Path source = resolveOrThrow(request.getOldPath());
            Path target = resolveOrThrow(request.getNewPath());

            try {
                Files.move(source, target);
            } catch (NoSuchFileException noSuchFileException) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "File does not exist, cannot rename");
            } catch (FileAlreadyExistsException alreadyExistsException) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "File already exists, cannot rename to replace");
            }
            responseObserver.onNext(MoveItemResponse.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void createDirectory(CreateDirectoryRequest request, StreamObserver<CreateDirectoryResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            Path dir = resolveOrThrow(request.getPath());
            try {
                Files.createDirectory(dir);
            } catch (FileAlreadyExistsException fileAlreadyExistsException) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Something already exists with that name");
            } catch (NoSuchFileException noSuchFileException) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Can't create directory, parent directory doesn't exist");
            }
            responseObserver.onNext(CreateDirectoryResponse.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void deleteItem(DeleteItemRequest request, StreamObserver<DeleteItemResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            Path path = resolveOrThrow(request.getPath());
            try {
                Files.delete(path);
            } catch (NoSuchFileException noSuchFileException) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Cannot delete, file does not exists");
            }
            responseObserver.onNext(DeleteItemResponse.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }
}
