//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.storage;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import elemental2.dom.Blob;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.CreateDirectoryRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.CreateDirectoryResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.DeleteItemRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.DeleteItemResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.FetchFileRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.FetchFileResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.ListItemsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.ListItemsResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.MoveItemRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.MoveItemResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.SaveFileRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.SaveFileResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb_service.StorageServiceClient;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.WorkerConnection;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsType;

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

    private StorageServiceClient client() {
        return connection.storageServiceClient();
    }

    private BrowserHeaders metadata() {
        return connection.metadata();
    }

    /**
     * Lists items in a given directory, with an optional filter glob to only list files that match. The empty or "root"
     * path should be specified as the empty string.
     *
     * @param path the path of the directory to list
     * @param glob optional glob to filter the contents of the directory
     * @return a promise containing the any items that are present in the given directory that match the glob, or an
     *         error.
     */
    @JsMethod
    public Promise<JsArray<JsItemDetails>> listItems(String path, @JsOptional String glob) {
        ListItemsRequest req = new ListItemsRequest();
        req.setPath(path);
        req.setFilterGlob(glob);
        return Callbacks.<ListItemsResponse, Object>grpcUnaryPromise(c -> client().listItems(req, metadata(), c::apply))
                .then(response -> Promise
                        .resolve(response.getItemsList()
                                .map((item, i) -> JsItemDetails.fromProto(response.getCanonicalPath(), item))));
    }

    /**
     * Downloads a file at the given path, unless an etag is provided that matches the file's current contents.
     *
     * @param path the path of the file to fetch
     * @param etag an optional etag from the last time the client saw this file
     * @return a promise containing details about the file's contents, or an error.
     */
    @JsMethod
    public Promise<JsFileContents> loadFile(String path, @JsOptional String etag) {
        FetchFileRequest req = new FetchFileRequest();
        req.setPath(path);
        if (etag != null) {
            req.setEtag(etag);
        }
        return Callbacks.<FetchFileResponse, Object>grpcUnaryPromise(c -> client().fetchFile(req, metadata(), c::apply))
                .then(response -> {
                    if (response.hasEtag() && response.getEtag().equals(etag)) {
                        return Promise.resolve(new JsFileContents(etag));
                    }
                    Blob contents = new Blob(JsArray.of(
                            Blob.ConstructorBlobPartsArrayUnionType.of(response.getContents_asU8().slice().buffer)));
                    return Promise.resolve(new JsFileContents(contents, response.getEtag()));
                });
    }

    /**
     * Deletes the item at the given path. Directories must be empty to be deleted.
     *
     * @param path the path of the item to delete
     * @return a promise with no value on success, or an error.
     */
    @JsMethod
    public Promise<Void> deleteItem(String path) {
        DeleteItemRequest req = new DeleteItemRequest();
        req.setPath(path);
        return Callbacks
                .<DeleteItemResponse, Object>grpcUnaryPromise(c -> client().deleteItem(req, metadata(), c::apply))
                .then(response -> Promise.resolve((Void) null));
    }

    /**
     * Saves the provided contents to the given path, creating a file or replacing an existing one. The optional newFile
     * parameter can be passed to indicate that an existing file must not be overwritten, only a new file created.
     *
     * Note that directories must be empty to be overwritten.
     *
     * @param path the path of the file to write
     * @param contents the contents to write to that path
     * @param allowOverwrite true to allow an existing file to be overwritten, false or skip to require a new file
     * @return a promise with a FileContents, holding only the new etag (if the server emitted one), or an error
     */
    @JsMethod
    public Promise<JsFileContents> saveFile(String path, JsFileContents contents, @JsOptional Boolean allowOverwrite) {
        return contents.arrayBuffer().then(ab -> {
            SaveFileRequest req = new SaveFileRequest();
            req.setContents(new Uint8Array(ab));
            req.setPath(path);

            if (allowOverwrite != null) {
                req.setAllowOverwrite(allowOverwrite);
            }

            return Callbacks
                    .<SaveFileResponse, Object>grpcUnaryPromise(c -> client().saveFile(req, metadata(), c::apply))
                    .then(response -> Promise.resolve(new JsFileContents(response.getEtag())));
        });
    }

    /**
     * Moves (and/or renames) an item from its old path to its new path. The optional newFile parameter can be passed to
     * enforce that an existing item must not be overwritten.
     *
     * Note that directories must be empty to be overwritten.
     *
     * @param oldPath the path of the existing item
     * @param newPath the new path to move the item to
     * @param allowOverwrite true to allow an existing file to be overwritten, false or skip to require a new file
     * @return a promise with no value on success, or an error.
     */
    @JsMethod
    public Promise<Void> moveItem(String oldPath, String newPath, @JsOptional Boolean allowOverwrite) {
        MoveItemRequest req = new MoveItemRequest();
        req.setOldPath(oldPath);
        req.setNewPath(newPath);

        if (allowOverwrite != null) {
            req.setAllowOverwrite(allowOverwrite);
        }

        return Callbacks.<MoveItemResponse, Object>grpcUnaryPromise(c -> client().moveItem(req, metadata(), c::apply))
                .then(response -> Promise.resolve((Void) null));
    }

    /**
     * Creates a new directory at the specified path.
     *
     * @param path the path of the directory to create
     * @return a promise with no value on success, or an error.
     */
    @JsMethod
    public Promise<Void> createDirectory(String path) {
        CreateDirectoryRequest req = new CreateDirectoryRequest();
        req.setPath(path);
        return Callbacks
                .<CreateDirectoryResponse, Object>grpcUnaryPromise(
                        c -> client().createDirectory(req, metadata(), c::apply))
                .then(response -> Promise.resolve((Void) null));
    }
}
