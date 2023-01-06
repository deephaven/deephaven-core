package io.deephaven.auth.codegen;

import com.google.common.base.Strings;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.compiler.PluginProtos;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import io.deephaven.auth.AuthContext;
import io.grpc.ServerServiceDefinition;

import javax.lang.model.element.Modifier;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class GenerateServiceAuthWiring {
    private static final String ON_CALL_STARTED = "onCallStarted";
    private static final String ON_MESSAGE_RECEIVED = "onMessageReceived";

    public static void main(String[] args) throws IOException {
        final PluginProtos.CodeGeneratorRequest request = PluginProtos.CodeGeneratorRequest.parseFrom(System.in);
        final PluginProtos.CodeGeneratorResponse.Builder response = PluginProtos.CodeGeneratorResponse.newBuilder();

        // tell protoc that we support proto3's optional as synthetic oneof feature
        response.setSupportedFeatures(PluginProtos.CodeGeneratorResponse.Feature.FEATURE_PROTO3_OPTIONAL_VALUE);

        // create a mapping from message type to java type name
        final Map<String, String> typeMap = generateTypeMap(request);

        // for each service, generate the auth wiring
        for (final DescriptorProtos.FileDescriptorProto file : request.getProtoFileList()) {
            final String realPackage = getRealPackage(file);
            for (final DescriptorProtos.ServiceDescriptorProto service : file.getServiceList()) {
                if (service.getName().contains("TableService")) {
                    // all table services perform an authorization check using contextual source tables
                    continue;
                } else if (service.getName().contains("BrowserFlightService")) {
                    // the browser flight requests get converted to FlightService requests based on our binding
                    continue;
                }
                generateForService(realPackage, response, service, typeMap);
            }
        }

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

    private static void generateForService(
            final String originalServicePackage,
            final PluginProtos.CodeGeneratorResponse.Builder response,
            final DescriptorProtos.ServiceDescriptorProto service,
            final Map<String, String> typeMap) throws IOException {
        final String packageName = "io.deephaven.auth.codegen.impl";
        final String serviceName = service.getName() + "AuthWiring";
        final ClassName resultInterface = ClassName.bestGuess(packageName + "." + serviceName);

        System.err.println("Generating: " + packageName + "." + serviceName);

        // create a default implementation that is permissive
        final TypeSpec.Builder allowAllSpec = TypeSpec.classBuilder("AllowAll")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .addSuperinterface(resultInterface);
        visitAllMethods(service, typeMap, allowAllSpec, (methodName, method) -> {
            // default impl is to do nothing
        });

        // create a default implementation that is restrictive
        final TypeSpec.Builder denyAllSpec = TypeSpec.classBuilder("DenyAll")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .addSuperinterface(resultInterface);
        visitAllMethods(service, typeMap, denyAllSpec, (methodName, method) -> {
            method.addCode("ServiceAuthWiring.operationNotAllowed();\n");
        });

        // create a test implementation with runtime-injectable behavior
        final TypeSpec.Builder testImplSpec = TypeSpec.classBuilder("TestUseOnly")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .addSuperinterface(resultInterface)
                .addField(FieldSpec.builder(resultInterface, "delegate")
                        .addModifiers(Modifier.PUBLIC)
                        .build());

        visitAllMethods(service, typeMap, testImplSpec, (methodName, method) -> {
            method.addCode("if (delegate != null) {\n");
            if (methodName.startsWith(ON_CALL_STARTED)) {
                method.addCode("  delegate." + methodName + "(authContext);\n");
            } else {
                method.addCode("  delegate." + methodName + "(authContext, request);\n");
            }
            method.addCode("}\n");
        });

        // this is a default method on the service interface that binds auth hooks to the rpc handlers
        final ClassName superClass = ClassName.bestGuess(
                originalServicePackage + "." + service.getName() + "Grpc." + service.getName() + "ImplBase");
        final ClassName serviceDefinitionClass = ClassName.get(ServerServiceDefinition.class);
        final MethodSpec.Builder interceptMethod = MethodSpec.methodBuilder("intercept")
                .returns(serviceDefinitionClass)
                .addModifiers(Modifier.PUBLIC, Modifier.DEFAULT)
                .addParameter(superClass, "delegate")
                .addCode("final $T service = delegate.bindService();\n", serviceDefinitionClass)
                .addCode("final $T.Builder serviceBuilder =\n", serviceDefinitionClass)
                .addCode("        $T.builder(service.getServiceDescriptor());\n", serviceDefinitionClass)
                .addCode("\n");

        for (final DescriptorProtos.MethodDescriptorProto method : service.getMethodList()) {
            final String name = method.getName();
            final String startCB = method.getClientStreaming() ? "this::" + ON_CALL_STARTED + name : "null";
            interceptMethod.addCode("serviceBuilder.addMethod(ServiceAuthWiring.intercept(\n")
                    .addCode("    service, $S, " + startCB + ", this::" + ON_MESSAGE_RECEIVED + name + "));\n", name);
        }

        interceptMethod.addCode("\nreturn serviceBuilder.build();\n");
        interceptMethod.addJavadoc("Wrap the real implementation with authorization checks.\n\n");
        interceptMethod.addJavadoc("@param delegate the real service implementation\n");
        interceptMethod.addJavadoc("@return the wrapped service implementation\n");

        final ParameterizedTypeName superInterface = ParameterizedTypeName.get(
                ClassName.bestGuess("io.deephaven.auth.ServiceAuthWiring"), superClass);
        final TypeSpec.Builder typeSpec = TypeSpec.interfaceBuilder(serviceName)
                .addModifiers(Modifier.PUBLIC)
                .addSuperinterface(superInterface)
                .addMethod(interceptMethod.build());
        visitAllMethods(service, typeMap, typeSpec, (methodName, method) -> {
            boolean hasRequest = false;

            method.addModifiers(Modifier.ABSTRACT);
            if (methodName.startsWith(ON_MESSAGE_RECEIVED)) {
                hasRequest = true;
                methodName = methodName.substring(ON_MESSAGE_RECEIVED.length());
                method.addJavadoc("Authorize a request to $L.\n\n", methodName);
            } else if (methodName.startsWith(ON_CALL_STARTED)) {
                methodName = methodName.substring(ON_CALL_STARTED.length());
                method.addJavadoc("Authorize a request to open a client-streaming rpc $L.\n\n", methodName);
            }

            method.addJavadoc("@param authContext the authentication context of the request\n");
            if (hasRequest) {
                method.addJavadoc("@param request the request to authorize\n");
            }
            method.addJavadoc("@throws io.grpc.StatusRuntimeException if the user is not authorized to invoke $L\n",
                    methodName);
        });

        typeSpec.addType(allowAllSpec.build());
        typeSpec.addType(denyAllSpec.build());
        typeSpec.addType(testImplSpec.build());

        typeSpec.addJavadoc("This interface provides type-safe authorization hooks for $L.\n",
                service.getName() + "Grpc");
        JavaFile javaFile = JavaFile.builder(packageName, typeSpec.build())
                .addFileComment("\n")
                .addFileComment("Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending\n")
                .addFileComment("\n")
                .addFileComment("------------------------------------------------------------------\n")
                .addFileComment("This class is generated by GenerateServiceAuthWiring. DO NOT EDIT!\n")
                .addFileComment("------------------------------------------------------------------\n")
                .build();

        final StringBuilder sb = new StringBuilder();
        javaFile.writeTo(sb);

        response.addFile(PluginProtos.CodeGeneratorResponse.File.newBuilder()
                .setName("io/deephaven/auth/codegen/impl/" + serviceName + ".java")
                .setContent(sb.toString())
                .build());
    }

    private static void visitAllMethods(
            DescriptorProtos.ServiceDescriptorProto service,
            Map<String, String> typeMap,
            TypeSpec.Builder typeSpec,
            BiConsumer<String, MethodSpec.Builder> visitor) {
        for (final DescriptorProtos.MethodDescriptorProto method : service.getMethodList()) {
            final String inputType = method.getInputType();
            final String realType = typeMap.get(inputType);
            if (realType == null) {
                System.err.println("Could not find type for: " + inputType);
                System.exit(-1);
            }

            if (method.getClientStreaming()) {
                final String methodName = ON_CALL_STARTED + method.getName();
                final MethodSpec.Builder methodSpec = MethodSpec.methodBuilder(methodName)
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(AuthContext.class, "authContext");
                visitor.accept(methodName, methodSpec);
                typeSpec.addMethod(methodSpec.build());
            }
            final String methodName = ON_MESSAGE_RECEIVED + method.getName();
            final MethodSpec.Builder methodSpec = MethodSpec.methodBuilder(methodName)
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(AuthContext.class, "authContext")
                    .addParameter(ClassName.bestGuess(realType), "request");
            visitor.accept(methodName, methodSpec);
            typeSpec.addMethod(methodSpec.build());
        }
    }

    private static String getRealPackage(final DescriptorProtos.FileDescriptorProto file) {
        String realPackage = null;
        if (file.hasOptions()) {
            realPackage = file.getOptions().getJavaPackage();
        }
        if (Strings.isNullOrEmpty(realPackage)) {
            realPackage = file.getPackage();
        }
        // Unsure of where this is specified in Flight.proto, but in addition to the package the messages are
        // put into a "Flight" namespace.
        if (realPackage.equals("org.apache.arrow.flight.impl")) {
            realPackage += ".Flight";
        }
        return realPackage;
    }
}
