package io.deephaven.auth.codegen;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.compiler.PluginProtos;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import io.deephaven.auth.AuthContext;
import io.deephaven.engine.table.Table;

import javax.lang.model.element.Modifier;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static io.deephaven.auth.codegen.GenerateServiceAuthWiring.generateTypeMap;

public class GenerateContextualAuthWiring {
    private static final String CHECK_PERMISSION = "checkPermission";

    public static void main(String[] args) throws IOException {
        final PluginProtos.CodeGeneratorRequest request = PluginProtos.CodeGeneratorRequest.parseFrom(System.in);
        final PluginProtos.CodeGeneratorResponse.Builder response = PluginProtos.CodeGeneratorResponse.newBuilder();

        // tell protoc that we support proto3's optional as synthetic oneof feature
        response.setSupportedFeatures(PluginProtos.CodeGeneratorResponse.Feature.FEATURE_PROTO3_OPTIONAL_VALUE);

        // create a mapping from message type to java type name
        final Map<String, String> typeMap = generateTypeMap(request);

        // for each service, generate the auth wiring
        for (final DescriptorProtos.FileDescriptorProto file : request.getProtoFileList()) {
            for (final DescriptorProtos.ServiceDescriptorProto service : file.getServiceList()) {
                // only table services perform an authorization check using contextual source tables
                generateForService(response, service, typeMap);
            }
        }

        response.build().toByteString().writeTo(System.out);
    }

    private static void generateForService(
            final PluginProtos.CodeGeneratorResponse.Builder response,
            final DescriptorProtos.ServiceDescriptorProto service,
            final Map<String, String> typeMap) throws IOException {
        final String packageName = "io.deephaven.auth.codegen.impl";
        final String serviceName = service.getName() + "ContextualAuthWiring";
        final String grpcServiceName = service.getName() + "Grpc";
        final ClassName resultInterface = ClassName.bestGuess(packageName + "." + serviceName);

        System.err.println("Generating: " + packageName + "." + serviceName);

        // create a default implementation that is permissive
        final TypeSpec.Builder allowAllSpec = TypeSpec.classBuilder("AllowAll")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .addSuperinterface(resultInterface);
        visitAllMethods(service, typeMap, allowAllSpec, (methodName, method) -> {
            // default impl is to do nothing
        });

        final ClassName authWiringClass = ClassName.bestGuess("io.deephaven.auth.ServiceAuthWiring");

        // create a default implementation that is restrictive
        final TypeSpec.Builder denyAllSpec = TypeSpec.classBuilder("DenyAll")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .addSuperinterface(resultInterface);
        visitAllMethods(service, typeMap, denyAllSpec, (methodName, method) -> {
            method.addCode("$T.operationNotAllowed();\n", authWiringClass);
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
            method.addCode("  delegate." + methodName + "(authContext, request, sourceTables);\n");
            method.addCode("}\n");
        });

        final TypeSpec.Builder typeSpec = TypeSpec.interfaceBuilder(serviceName)
                .addModifiers(Modifier.PUBLIC);
        visitAllMethods(service, typeMap, typeSpec, (methodName, method) -> {
            method.addModifiers(Modifier.ABSTRACT);
            methodName = methodName.substring(CHECK_PERMISSION.length());
            method.addJavadoc("Authorize a request to $L.\n\n", methodName);
            method.addJavadoc("@param authContext the authentication context of the request\n");
            method.addJavadoc("@param request the request to authorize\n");
            method.addJavadoc("@param sourceTables the operation's source tables\n");
            method.addJavadoc("@throws io.grpc.StatusRuntimeException if the user is not authorized to invoke $L\n",
                    methodName);
        });

        typeSpec.addType(allowAllSpec.build());
        typeSpec.addType(denyAllSpec.build());
        typeSpec.addType(testImplSpec.build());
        typeSpec.addJavadoc("This interface provides type-safe authorization hooks for $L.\n",
                grpcServiceName);
        JavaFile javaFile = JavaFile.builder(packageName, typeSpec.build())
                .addFileComment("\n")
                .addFileComment("Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending\n")
                .addFileComment("\n")
                .addFileComment("---------------------------------------------------------------------\n")
                .addFileComment("This class is generated by GenerateContextualAuthWiring. DO NOT EDIT!\n")
                .addFileComment("---------------------------------------------------------------------\n")
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
            if (method.getName().equals("Batch")) {
                // batch methods get broken up and will authorize each operation individually
                continue;
            }

            // strip off the leading '.', but otherwise we do not do anything fancy with the java package
            final String inputType = method.getInputType();
            final String realType = typeMap.get(inputType);
            if (realType == null) {
                System.err.println("Could not find type for: " + inputType);
                System.exit(-1);
            }

            final String checkMethodName = CHECK_PERMISSION + method.getName();
            final MethodSpec.Builder methodSpec = MethodSpec.methodBuilder(checkMethodName)
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(AuthContext.class, "authContext")
                    .addParameter(ClassName.bestGuess(realType), "request")
                    .addParameter(ParameterizedTypeName.get(List.class, Table.class), "sourceTables");
            visitor.accept(checkMethodName, methodSpec);
            typeSpec.addMethod(methodSpec.build());
        }
    }
}
