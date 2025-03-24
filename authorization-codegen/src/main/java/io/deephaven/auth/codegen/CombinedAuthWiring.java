//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.auth.codegen;

import com.google.common.base.Strings;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.compiler.PluginProtos;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CombinedAuthWiring {
    public static void main(String[] args) throws IOException {
        // First, initial setup to be shared by both plugin aspects
        final PluginProtos.CodeGeneratorRequest request = PluginProtos.CodeGeneratorRequest.parseFrom(System.in);
        final PluginProtos.CodeGeneratorResponse.Builder response = PluginProtos.CodeGeneratorResponse.newBuilder();

        // tell protoc that we support proto3's optional as synthetic oneof feature
        response.setSupportedFeatures(PluginProtos.CodeGeneratorResponse.Feature.FEATURE_PROTO3_OPTIONAL_VALUE);

        // create a mapping from message type to java type name
        final Map<String, String> typeMap = generateTypeMap(request);

        // Next, for each service, generate the auth wiring
        for (final DescriptorProtos.FileDescriptorProto file : request.getProtoFileList()) {
            if (!request.getFileToGenerateList().contains(file.getName())) {
                // Skip, this file wasn't requested to be generated, just included as a dependency
                continue;
            }
            final String realPackage = getRealPackage(file);
            for (final DescriptorProtos.ServiceDescriptorProto service : file.getServiceList()) {
                if (service.getName().contains("BrowserFlightService")) {
                    // the browser flight requests get converted to FlightService requests based on our binding
                    continue;
                }

                // Only table services perform an authorization check using contextual source tables
                // In other circumstances we would generate from .proto first, then compile this plugin, then
                // run the plugin on the remaining .proto files, but we aren't generalizing the plugin that far
                // at this time.
                List<Long> contextualAuthValue =
                        service.getOptions().getUnknownFields().getField(0x6E68).getVarintList();
                boolean hasContextualAuth = !contextualAuthValue.isEmpty() && contextualAuthValue.get(0) == 1;
                if (hasContextualAuth) {
                    GenerateContextualAuthWiring.generateForService(response, service, typeMap);
                } else {
                    GenerateServiceAuthWiring.generateForService(realPackage, response, service, typeMap);
                }
            }
        }

        // Finally, write the combined output back to protoc
        response.build().toByteString().writeTo(System.out);
    }

    public static Map<String, String> generateTypeMap(PluginProtos.CodeGeneratorRequest request) {
        final Map<String, String> typeMap = new HashMap<>();
        for (final DescriptorProtos.FileDescriptorProto file : request.getProtoFileList()) {
            String realPackage = getRealPackage(file);
            for (final DescriptorProtos.DescriptorProto message : file.getMessageTypeList()) {
                typeMap.put("." + file.getPackage() + "." + message.getName(), realPackage + "." + message.getName());
            }
        }
        return typeMap;
    }

    public static String getRealPackage(final DescriptorProtos.FileDescriptorProto file) {
        String realPackage = null;
        if (file.hasOptions()) {
            realPackage = file.getOptions().getJavaPackage();
        }
        if (Strings.isNullOrEmpty(realPackage)) {
            realPackage = file.getPackage();
        }
        return realPackage;
    }
}
