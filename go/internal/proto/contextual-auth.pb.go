//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v4.25.3
// source: deephaven_core/proto/contextual-auth.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	descriptorpb "google.golang.org/protobuf/types/descriptorpb"
	reflect "reflect"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

var file_deephaven_core_proto_contextual_auth_proto_extTypes = []protoimpl.ExtensionInfo{
	{
		ExtendedType:  (*descriptorpb.ServiceOptions)(nil),
		ExtensionType: (*bool)(nil),
		Field:         28264,
		Name:          "io.deephaven.proto.backplane.grpc.contextual_auth",
		Tag:           "varint,28264,opt,name=contextual_auth",
		Filename:      "deephaven_core/proto/contextual-auth.proto",
	},
}

// Extension fields to descriptorpb.ServiceOptions.
var (
	// Indicates that the service requires server-side contextual authorization checks.
	// (Numeric value is 'dh', to avoid collisions with nice round generated numbers.)
	//
	// optional bool contextual_auth = 28264;
	E_ContextualAuth = &file_deephaven_core_proto_contextual_auth_proto_extTypes[0]
)

var File_deephaven_core_proto_contextual_auth_proto protoreflect.FileDescriptor

var file_deephaven_core_proto_contextual_auth_proto_rawDesc = []byte{
	0x0a, 0x2a, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e, 0x5f, 0x63, 0x6f, 0x72, 0x65,
	0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x75, 0x61,
	0x6c, 0x2d, 0x61, 0x75, 0x74, 0x68, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x21, 0x69, 0x6f,
	0x2e, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x2e, 0x62, 0x61, 0x63, 0x6b, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x1a,
	0x20, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66,
	0x2f, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x6f, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x3a, 0x4a, 0x0a, 0x0f, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x75, 0x61, 0x6c, 0x5f,
	0x61, 0x75, 0x74, 0x68, 0x12, 0x1f, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x4f, 0x70,
	0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0xe8, 0xdc, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0e, 0x63,
	0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x75, 0x61, 0x6c, 0x41, 0x75, 0x74, 0x68, 0x42, 0x3b, 0x48,
	0x01, 0x50, 0x01, 0x5a, 0x35, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e, 0x2f, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61,
	0x76, 0x65, 0x6e, 0x2d, 0x63, 0x6f, 0x72, 0x65, 0x2f, 0x67, 0x6f, 0x2f, 0x69, 0x6e, 0x74, 0x65,
	0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x33,
}

var file_deephaven_core_proto_contextual_auth_proto_goTypes = []interface{}{
	(*descriptorpb.ServiceOptions)(nil), // 0: google.protobuf.ServiceOptions
}
var file_deephaven_core_proto_contextual_auth_proto_depIdxs = []int32{
	0, // 0: io.deephaven.proto.backplane.grpc.contextual_auth:extendee -> google.protobuf.ServiceOptions
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	0, // [0:1] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_deephaven_core_proto_contextual_auth_proto_init() }
func file_deephaven_core_proto_contextual_auth_proto_init() {
	if File_deephaven_core_proto_contextual_auth_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_deephaven_core_proto_contextual_auth_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   0,
			NumExtensions: 1,
			NumServices:   0,
		},
		GoTypes:           file_deephaven_core_proto_contextual_auth_proto_goTypes,
		DependencyIndexes: file_deephaven_core_proto_contextual_auth_proto_depIdxs,
		ExtensionInfos:    file_deephaven_core_proto_contextual_auth_proto_extTypes,
	}.Build()
	File_deephaven_core_proto_contextual_auth_proto = out.File
	file_deephaven_core_proto_contextual_auth_proto_rawDesc = nil
	file_deephaven_core_proto_contextual_auth_proto_goTypes = nil
	file_deephaven_core_proto_contextual_auth_proto_depIdxs = nil
}
