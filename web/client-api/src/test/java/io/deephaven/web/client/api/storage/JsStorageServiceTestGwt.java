//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.storage;

import elemental2.promise.Promise;
import io.deephaven.web.client.api.AbstractAsyncGwtTestCase;
import io.deephaven.web.client.api.CoreClient;
import jsinterop.base.JsPropertyMap;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class JsStorageServiceTestGwt extends AbstractAsyncGwtTestCase {

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

                // TODO (deephaven-core#5068) remove leading slash
                Set<String> expected = new HashSet<>(Arrays.asList("layouts", "notebooks"));
                for (int i = 0; i < items.length; i++) {
                    expected.remove(items.getAt(i).getBasename());
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
                    delayTestFinish(20_004);

                    assertEquals(1, items.length);

                    assertEquals(JsItemType.DIRECTORY, items.getAt(0).getType());
                    assertEquals("myDir", items.getAt(0).getBasename());
                    return null;
                }), storageService.listItems("notebooks/", null).then(items -> {
                    delayTestFinish(20_005);

                    assertEquals(1, items.length);

                    assertEquals(JsItemType.DIRECTORY, items.getAt(0).getType());
                    assertEquals("myDir", items.getAt(0).getBasename());
                    return null;
                }), storageService.listItems("/notebooks/", null).then(items -> {
                    delayTestFinish(20_006);

                    assertEquals(1, items.length);

                    assertEquals(JsItemType.DIRECTORY, items.getAt(0).getType());
                    assertEquals("myDir", items.getAt(0).getBasename());
                    return null;
                }));
            }).then(ignore -> storageService
                    .saveFile("layouts/myFile.txt", JsFileContents.text("hello, ", "world"), false)
                    .then(file -> {
                        delayTestFinish(20_007);

                        assertEquals("8ebc5e3a62ac2f344d41429607bcdc4c", file.getEtag());
                        return Promise.all(storageService.listItems("layouts", null)
                                .then(items -> {
                                    delayTestFinish(20_008);

                                    assertEquals(JsItemType.FILE, items.getAt(0).getType());
                                    assertEquals("myFile.txt", items.getAt(0).getBasename());
                                    return null;
                                }), storageService.loadFile("layouts/myFile.txt", null).then(contents -> {
                                    delayTestFinish(20_009);

                                    return contents.text().then(c -> {
                                        delayTestFinish(20_010);

                                        assertEquals("hello, world", c);
                                        return null;
                                    });
                                }), storageService.loadFile("/layouts/myFile.txt", null).then(contents -> {
                                    delayTestFinish(20_011);

                                    return contents.text().then(c -> {
                                        delayTestFinish(20_012);

                                        assertEquals("hello, world", c);
                                        return null;
                                    });
                                }), storageService.loadFile("layouts/myFile.txt", "8ebc5e3a62ac2f344d41429607bcdc4c")
                                        .then(contents -> {
                                            delayTestFinish(20_013);

                                            return contents.text().then(c -> {
                                                fail("should not have resolved");
                                                return null;
                                            }, error -> {
                                                assertTrue(error.toString()
                                                        .contains("No contents available, please use provided etag"));
                                                return null;
                                            });
                                        }),
                                storageService.loadFile("layouts/myFile.txt", "definitely-the-wrong-hash")
                                        .then(contents -> {
                                            delayTestFinish(20_014);

                                            return contents.text().then(c -> {
                                                delayTestFinish(20_015);

                                                assertEquals("hello, world", c);
                                                return null;
                                            });
                                        }));
                    })).then(ignore -> {
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
