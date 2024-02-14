/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.examples;

import io.deephaven.client.impl.TicketId;
import io.deephaven.client.impl.TypedTicket;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.TypeConversionException;

import java.nio.charset.StandardCharsets;

class TypedTicketConverter implements ITypeConverter<TypedTicket> {

    @Override
    public TypedTicket convert(String value) {
        final String[] split = value.split(":");
        if (split.length != 2) {
            throw new TypeConversionException("Expected typed ticket to be of the form '<type>:<ticket>'");
        }
        return new TypedTicket(split[0], new TicketId(split[1].getBytes(StandardCharsets.UTF_8)));
    }
}
