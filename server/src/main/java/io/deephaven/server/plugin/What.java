package io.deephaven.server.plugin;

import io.deephaven.proto.backplane.grpc.FieldInfo.FieldType;

public class What {
    public static void main(String[] args) {

        System.out.println(FieldType.getDefaultInstance().getFieldCase());
    }
}
