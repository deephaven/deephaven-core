package io.deephaven.web.client.api.storage;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.CreateDirectoryRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.CreateDirectoryResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.DeleteItemRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.DeleteItemResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.FetchFileRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.FetchFileResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.ListItemsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.ListItemsResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.MoveItemRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.MoveItemResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.SaveFileRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.SaveFileResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb_service.NotebookServiceClient;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.WorkerConnection;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;

public class StorageService {
    private final WorkerConnection connection;

    public StorageService(WorkerConnection connection) {
        this.connection = connection;
    }

    private NotebookServiceClient client() {
        return connection.notebookServiceClient();
    }

    private BrowserHeaders metadata() {
        return connection.metadata();
    }

    @JsMethod
    public Promise<JsArray<ItemDetails>> listItems(String path, @JsOptional String glob) {
        ListItemsRequest req = new ListItemsRequest();
        req.setPath(path);
        req.setFilterGlob(glob);
        return Callbacks.<ListItemsResponse, Object>grpcUnaryPromise(c -> client().listItems(req, metadata(), c::apply))
                .then(response -> Promise.resolve(response.getItemsList().map((item, i, arr) -> ItemDetails.fromProto(item))));
    }

    @JsMethod
    public Promise<FileContents> loadFile(String path) {
        FetchFileRequest req = new FetchFileRequest();
        req.setPath(path);
        return Callbacks.<FetchFileResponse, Object>grpcUnaryPromise(c -> client().fetchFile(req, metadata(), c::apply))
                .then(response -> Promise.resolve(FileContents.arrayBuffers(response.serializeBinary().buffer)));
    }

    @JsMethod
    public Promise<Void> deleteItem(String path) {
        DeleteItemRequest req = new DeleteItemRequest();
        req.setPath(path);
        return Callbacks.<DeleteItemResponse, Object>grpcUnaryPromise(c -> client().deleteItem(req, metadata(), c::apply))
                .then(response -> Promise.resolve((Void)null));
    }

    @JsMethod
    public Promise<Void> saveFile(String path, FileContents contents) {
        return contents.arrayBuffer().then(ab -> {
            SaveFileRequest req = new SaveFileRequest();
            req.setContents(new Uint8Array(ab));
            req.setPath(path);
            return Callbacks.<SaveFileResponse, Object>grpcUnaryPromise(c -> client().saveFile(req, metadata(), c::apply))
                    .then(response -> Promise.resolve((Void)null));
        });
    }

    @JsMethod
    public Promise<Void> moveItem(String oldPath, String newPath) {
        MoveItemRequest req = new MoveItemRequest();
        req.setOldPath(oldPath);
        req.setNewPath(newPath);
        return Callbacks.<MoveItemResponse, Object>grpcUnaryPromise(c -> client().moveItem(req, metadata(), c::apply))
                .then(response -> Promise.resolve((Void)null));
    }

    @JsMethod
    public Promise<Void> createDirectory(String path) {
        CreateDirectoryRequest req = new CreateDirectoryRequest();
        req.setPath(path);
        return Callbacks.<CreateDirectoryResponse, Object>grpcUnaryPromise(c -> client().createDirectory(req, metadata(), c::apply))
                .then(response -> Promise.resolve((Void)null));
    }
}
