package io.deephaven.web;

import com.google.auto.service.AutoService;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeClassBase;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;


@AutoService(ObjectType.class)
public final class WebLayoutType extends ObjectTypeClassBase<WebLayout> {
    public WebLayoutType() {
        super(WebLayout.class.getName(), WebLayout.class);
    }

    @Override
    public void writeToImpl(Exporter exporter, WebLayout layout, OutputStream out) throws IOException {
        Files.copy(layout.path(), out);
    }
}
