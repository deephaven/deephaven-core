//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.storage;

import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringAccess;
import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import elemental2.dom.Blob;
import elemental2.promise.Promise;
import io.deephaven.proto.backplane.grpc.CreateDirectoryRequest;
import io.deephaven.proto.backplane.grpc.CreateDirectoryResponse;
import io.deephaven.proto.backplane.grpc.DeleteItemRequest;
import io.deephaven.proto.backplane.grpc.DeleteItemResponse;
import io.deephaven.proto.backplane.grpc.FetchFileRequest;
import io.deephaven.proto.backplane.grpc.FetchFileResponse;
import io.deephaven.proto.backplane.grpc.ListItemsRequest;
import io.deephaven.proto.backplane.grpc.ListItemsResponse;
import io.deephaven.proto.backplane.grpc.MoveItemRequest;
import io.deephaven.proto.backplane.grpc.MoveItemResponse;
import io.deephaven.proto.backplane.grpc.SaveFileRequest;
import io.deephaven.proto.backplane.grpc.SaveFileResponse;
import io.deephaven.proto.backplane.grpc.StorageServiceGrpc;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.fu.JsCollectors;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;
import org.gwtproject.nio.TypedArrayHelper;

/**
 * Remote service to read and write files on the server. Paths use "/" as a separator, and should not start with "/".
 */
@JsType(namespace = "dh.storage", name = "StorageService")
public class JsStorageService {
    private final WorkerConnection connection;

    @JsIgnore
    public JsStorageService(WorkerConnection connection) {
        this.connection = connection;
    }

    private StorageServiceGrpc.StorageServiceStub client() {
        return connection.storageServiceClient();
    }

    /**
     * Lists items in a given directory, with an optional filter glob to only list files that match. The empty or "root"
     * path should be specified as the empty string.
     *
     * @param path the path of the directory to list.
     * @param glob optional glob to filter the contents of the directory.
     * @return a promise containing the any items that are present in the given directory that match the glob, or an
     *         error.
     */
    @JsMethod
    public Promise<JsArray<JsItemDetails>> listItems(String path, @JsOptional @JsNullable String glob) {
        ListItemsRequest.Builder builder = ListItemsRequest.newBuilder()
                .setPath(path);
        if (glob != null) {
            builder.setFilterGlob(glob);
        }
        return Callbacks.<ListItemsResponse>grpcUnaryPromise(c -> client().listItems(builder.build(), c))
                .then(response -> Promise
                        .resolve(response.getItemsList()
                                .stream()
                                .map(item -> JsItemDetails.fromProto(response.getCanonicalPath(), item))
                                .collect(JsCollectors.toFrozenJsArray())));
    }

    /**
     * Downloads a file at the given path, unless an etag is provided that matches the file's current contents.
     *
     * @param path the path of the file to fetch.
     * @param etag an optional etag from the last time the client saw this file.
     * @return a promise containing details about the file's contents, or an error.
     */
    @JsMethod
    public Promise<JsFileContents> loadFile(String path, @JsOptional @JsNullable String etag) {
        FetchFileRequest.Builder req = FetchFileRequest.newBuilder();
        req.setPath(path);
        if (etag != null) {
            req.setEtag(etag);
        }
        return Callbacks.<FetchFileResponse>grpcUnaryPromise(c -> client().fetchFile(req.build(), c))
                .then(response -> {
                    if (response.hasEtag() && response.getEtag().equals(etag)) {
                        return Promise.resolve(new JsFileContents(etag));
                    }
                    ByteString contents = response.getContents();
                    Uint8Array bytes = new Uint8Array(contents.size());
                    for (int i = 0; i < contents.size(); i++) {
                        bytes.setAt(i, (double) contents.byteAt(i));
                    }
                    Blob blob = new Blob(JsArray.of(
                            Blob.ConstructorBlobPartsArrayUnionType.of(bytes.buffer)));
                    return Promise.resolve(new JsFileContents(blob, response.getEtag()));
                });
    }

    /**
     * Deletes the item at the given path. Directories must be empty to be deleted.
     *
     * @param path the path of the item to delete.
     * @return a promise with no value on success, or an error.
     */
    @JsMethod
    public Promise<Void> deleteItem(String path) {
        DeleteItemRequest req = DeleteItemRequest.newBuilder()
                .setPath(path)
                .build();
        return Callbacks
                .<DeleteItemResponse>grpcUnaryPromise(c -> client().deleteItem(req, c))
                .then(response -> Promise.resolve((Void) null));
    }

    /**
     * Saves the provided contents to the given path, creating a file or replacing an existing one. The optional
     * {@code newFile} parameter can be passed to indicate that an existing file must not be overwritten, only a new
     * file created.
     * 
     * Note that directories must be empty to be overwritten.
     *
     * @param path the path of the file to write.
     * @param contents the contents to write to that path.
     * @param allowOverwrite {@code true} to allow an existing file to be overwritten, {@code false} or skip to require
     *        a new file.
     * @return a promise with a {@code FileContents}, holding only the new etag (if the server emitted one), or an
     *         error.
     */
    @JsMethod
    public Promise<JsFileContents> saveFile(String path, JsFileContents contents,
            @JsOptional @JsNullable Boolean allowOverwrite) {
        return contents.arrayBuffer().then(ab -> {
            SaveFileRequest.Builder req = SaveFileRequest.newBuilder();
            req.setContents(ByteStringAccess.wrap(TypedArrayHelper.wrap(ab)));
            req.setPath(path);

            if (allowOverwrite != null) {
                req.setAllowOverwrite(allowOverwrite);
            }

            return Callbacks
                    .<SaveFileResponse>grpcUnaryPromise(c -> client().saveFile(req.build(), c))
                    .then(response -> Promise.resolve(new JsFileContents(response.getEtag())));
        });
    }

    /**
     * Moves (and/or renames) an item from its old path to its new path. The optional {@code newFile} parameter can be
     * passed to enforce that an existing item must not be overwritten.
     *
     * Note that directories must be empty to be overwritten.
     *
     * @param oldPath the path of the existing item.
     * @param newPath the new path to move the item to.
     * @param allowOverwrite true to allow an existing file to be overwritten, false or skip to require a new file
     * @return a promise with no value on success, or an error.
     */
    @JsMethod
    public Promise<Void> moveItem(String oldPath, String newPath, @JsOptional @JsNullable Boolean allowOverwrite) {
        MoveItemRequest.Builder req = MoveItemRequest.newBuilder();
        req.setOldPath(oldPath);
        req.setNewPath(newPath);

        if (allowOverwrite != null) {
            req.setAllowOverwrite(allowOverwrite);
        }

        return Callbacks.<MoveItemResponse>grpcUnaryPromise(c -> client().moveItem(req.build(), c))
                .then(response -> Promise.resolve((Void) null));
    }

    /**
     * Creates a new directory at the specified path.
     *
     * @param path the path of the directory to create.
     * @return a promise with no value on success, or an error.
     */
    @JsMethod
    public Promise<Void> createDirectory(String path) {
        CreateDirectoryRequest.Builder req = CreateDirectoryRequest.newBuilder();
        req.setPath(path);
        return Callbacks
                .<CreateDirectoryResponse>grpcUnaryPromise(
                        c -> client().createDirectory(req.build(), c))
                .then(response -> Promise.resolve((Void) null));
    }
}
