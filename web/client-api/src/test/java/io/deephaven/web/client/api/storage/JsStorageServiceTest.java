package io.deephaven.web.client.api.storage;

import elemental2.promise.Promise;
import io.deephaven.web.client.api.AbstractAsyncGwtTestCase;
import io.deephaven.web.client.api.CoreClient;
import jsinterop.base.JsPropertyMap;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class JsStorageServiceTest extends AbstractAsyncGwtTestCase {

    public void testStorageService() {
        setupDhInternal().then(ignore -> {
            CoreClient coreClient = new CoreClient(localServer, null);
            return coreClient.login(JsPropertyMap.of("type", CoreClient.LOGIN_TYPE_ANONYMOUS))
                    .then(ignore2 -> Promise.resolve(coreClient));
        }).then(coreClient -> {
            JsStorageService storageService = coreClient.getStorageService();
            delayTestFinish(20_000);

            // On startup, there are only default items
            return storageService.listItems("", null).then(items -> {
                assertEquals(items.toString_(), 2, items.length);

                Set<String> expected = new HashSet<>(Arrays.asList("layouts", "notebooks"));
                for (int i = 0; i < items.length; i++) {
                    expected.remove(items.getAt(i).getFilename());
                }
                assertTrue(expected.toString(), expected.isEmpty());

                delayTestFinish(20_001);
                return storageService.createDirectory("notebooks/myDir");
            }).then(ignore -> {
                delayTestFinish(20_002);

                return Promise.all(storageService.listItems("notebooks", null).then(items -> {
                    delayTestFinish(20_003);

                    assertEquals(1, items.length);

                    assertEquals(JsItemType.DIRECTORY, items.getAt(0).getType());
                    assertEquals("myDir", items.getAt(0).getBasename());

                    return null;
                }), storageService.listItems("/notebooks", null).then(items -> {
                    delayTestFinish(20_003);

                    assertEquals(1, items.length);

                    assertEquals(JsItemType.DIRECTORY, items.getAt(0).getType());
                    assertEquals("myDir", items.getAt(0).getBasename());
                    return null;
                }), storageService.listItems("notebooks/", null).then(items -> {
                    delayTestFinish(20_003);

                    assertEquals(1, items.length);

                    assertEquals(JsItemType.DIRECTORY, items.getAt(0).getType());
                    assertEquals("myDir", items.getAt(0).getBasename());
                    return null;
                }), storageService.listItems("/notebooks/", null).then(items -> {
                    delayTestFinish(20_003);

                    assertEquals(1, items.length);

                    assertEquals(JsItemType.DIRECTORY, items.getAt(0).getType());
                    assertEquals("myDir", items.getAt(0).getBasename());
                    return null;
                }));
            }).then(ignore -> {
                delayTestFinish(20_003);




                finishTest();
                return null;
            }).catch_(this::report);
        });
    }

    @Override
    public String getModuleName() {
        return "io.deephaven.web.DeephavenIntegrationTest";
    }
}