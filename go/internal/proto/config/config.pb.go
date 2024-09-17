//
// Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v5.28.1
// source: deephaven/proto/config.proto

package config

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type AuthenticationConstantsRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *AuthenticationConstantsRequest) Reset() {
	*x = AuthenticationConstantsRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_deephaven_proto_config_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AuthenticationConstantsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AuthenticationConstantsRequest) ProtoMessage() {}

func (x *AuthenticationConstantsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_deephaven_proto_config_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AuthenticationConstantsRequest.ProtoReflect.Descriptor instead.
func (*AuthenticationConstantsRequest) Descriptor() ([]byte, []int) {
	return file_deephaven_proto_config_proto_rawDescGZIP(), []int{0}
}

type ConfigurationConstantsRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *ConfigurationConstantsRequest) Reset() {
	*x = ConfigurationConstantsRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_deephaven_proto_config_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ConfigurationConstantsRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ConfigurationConstantsRequest) ProtoMessage() {}

func (x *ConfigurationConstantsRequest) ProtoReflect() protoreflect.Message {
	mi := &file_deephaven_proto_config_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ConfigurationConstantsRequest.ProtoReflect.Descriptor instead.
func (*ConfigurationConstantsRequest) Descriptor() ([]byte, []int) {
	return file_deephaven_proto_config_proto_rawDescGZIP(), []int{1}
}

type AuthenticationConstantsResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ConfigValues map[string]*ConfigValue `protobuf:"bytes,1,rep,name=config_values,json=configValues,proto3" json:"config_values,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *AuthenticationConstantsResponse) Reset() {
	*x = AuthenticationConstantsResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_deephaven_proto_config_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AuthenticationConstantsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AuthenticationConstantsResponse) ProtoMessage() {}

func (x *AuthenticationConstantsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_deephaven_proto_config_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AuthenticationConstantsResponse.ProtoReflect.Descriptor instead.
func (*AuthenticationConstantsResponse) Descriptor() ([]byte, []int) {
	return file_deephaven_proto_config_proto_rawDescGZIP(), []int{2}
}

func (x *AuthenticationConstantsResponse) GetConfigValues() map[string]*ConfigValue {
	if x != nil {
		return x.ConfigValues
	}
	return nil
}

type ConfigurationConstantsResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ConfigValues map[string]*ConfigValue `protobuf:"bytes,1,rep,name=config_values,json=configValues,proto3" json:"config_values,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *ConfigurationConstantsResponse) Reset() {
	*x = ConfigurationConstantsResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_deephaven_proto_config_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ConfigurationConstantsResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ConfigurationConstantsResponse) ProtoMessage() {}

func (x *ConfigurationConstantsResponse) ProtoReflect() protoreflect.Message {
	mi := &file_deephaven_proto_config_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ConfigurationConstantsResponse.ProtoReflect.Descriptor instead.
func (*ConfigurationConstantsResponse) Descriptor() ([]byte, []int) {
	return file_deephaven_proto_config_proto_rawDescGZIP(), []int{3}
}

func (x *ConfigurationConstantsResponse) GetConfigValues() map[string]*ConfigValue {
	if x != nil {
		return x.ConfigValues
	}
	return nil
}

type ConfigValue struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Using a oneof leaves room for more types. The string_value tag is used here to allow for "interop" with
	// struct.proto.
	//
	// Types that are assignable to Kind:
	//	*ConfigValue_StringValue
	Kind isConfigValue_Kind `protobuf_oneof:"kind"`
}

func (x *ConfigValue) Reset() {
	*x = ConfigValue{}
	if protoimpl.UnsafeEnabled {
		mi := &file_deephaven_proto_config_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ConfigValue) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ConfigValue) ProtoMessage() {}

func (x *ConfigValue) ProtoReflect() protoreflect.Message {
	mi := &file_deephaven_proto_config_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ConfigValue.ProtoReflect.Descriptor instead.
func (*ConfigValue) Descriptor() ([]byte, []int) {
	return file_deephaven_proto_config_proto_rawDescGZIP(), []int{4}
}

func (m *ConfigValue) GetKind() isConfigValue_Kind {
	if m != nil {
		return m.Kind
	}
	return nil
}

func (x *ConfigValue) GetStringValue() string {
	if x, ok := x.GetKind().(*ConfigValue_StringValue); ok {
		return x.StringValue
	}
	return ""
}

type isConfigValue_Kind interface {
	isConfigValue_Kind()
}

type ConfigValue_StringValue struct {
	StringValue string `protobuf:"bytes,3,opt,name=string_value,json=stringValue,proto3,oneof"`
}

func (*ConfigValue_StringValue) isConfigValue_Kind() {}

var File_deephaven_proto_config_proto protoreflect.FileDescriptor

var file_deephaven_proto_config_proto_rawDesc = []byte{
	0x0a, 0x1c, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e, 0x2f, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x2f, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x21,
	0x69, 0x6f, 0x2e, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x2e, 0x62, 0x61, 0x63, 0x6b, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x67, 0x72, 0x70,
	0x63, 0x22, 0x20, 0x0a, 0x1e, 0x41, 0x75, 0x74, 0x68, 0x65, 0x6e, 0x74, 0x69, 0x63, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x43, 0x6f, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x74, 0x73, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x22, 0x1f, 0x0a, 0x1d, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x43, 0x6f, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x74, 0x73, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x22, 0x8d, 0x02, 0x0a, 0x1f, 0x41, 0x75, 0x74, 0x68, 0x65, 0x6e, 0x74,
	0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x43, 0x6f, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x74, 0x73,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x79, 0x0a, 0x0d, 0x63, 0x6f, 0x6e, 0x66,
	0x69, 0x67, 0x5f, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32,
	0x54, 0x2e, 0x69, 0x6f, 0x2e, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x62, 0x61, 0x63, 0x6b, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x67,
	0x72, 0x70, 0x63, 0x2e, 0x41, 0x75, 0x74, 0x68, 0x65, 0x6e, 0x74, 0x69, 0x63, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x43, 0x6f, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x74, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x2e, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73,
	0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x0c, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x56, 0x61, 0x6c,
	0x75, 0x65, 0x73, 0x1a, 0x6f, 0x0a, 0x11, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x56, 0x61, 0x6c,
	0x75, 0x65, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x44, 0x0a, 0x05, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x2e, 0x2e, 0x69, 0x6f, 0x2e, 0x64,
	0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x62,
	0x61, 0x63, 0x6b, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x43, 0x6f,
	0x6e, 0x66, 0x69, 0x67, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x3a, 0x02, 0x38, 0x01, 0x22, 0x8b, 0x02, 0x0a, 0x1e, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75,
	0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x43, 0x6f, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x74, 0x73, 0x52,
	0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x78, 0x0a, 0x0d, 0x63, 0x6f, 0x6e, 0x66, 0x69,
	0x67, 0x5f, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x53,
	0x2e, 0x69, 0x6f, 0x2e, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x2e, 0x62, 0x61, 0x63, 0x6b, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x67, 0x72,
	0x70, 0x63, 0x2e, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x43, 0x6f, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x74, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x2e, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x45, 0x6e,
	0x74, 0x72, 0x79, 0x52, 0x0c, 0x63, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x56, 0x61, 0x6c, 0x75, 0x65,
	0x73, 0x1a, 0x6f, 0x0a, 0x11, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x56, 0x61, 0x6c, 0x75, 0x65,
	0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x44, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x2e, 0x2e, 0x69, 0x6f, 0x2e, 0x64, 0x65, 0x65,
	0x70, 0x68, 0x61, 0x76, 0x65, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x62, 0x61, 0x63,
	0x6b, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x43, 0x6f, 0x6e, 0x66,
	0x69, 0x67, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02,
	0x38, 0x01, 0x22, 0x3a, 0x0a, 0x0b, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x56, 0x61, 0x6c, 0x75,
	0x65, 0x12, 0x23, 0x0a, 0x0c, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x5f, 0x76, 0x61, 0x6c, 0x75,
	0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x0b, 0x73, 0x74, 0x72, 0x69, 0x6e,
	0x67, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x42, 0x06, 0x0a, 0x04, 0x6b, 0x69, 0x6e, 0x64, 0x32, 0xdc,
	0x02, 0x0a, 0x0d, 0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65,
	0x12, 0xa5, 0x01, 0x0a, 0x1a, 0x47, 0x65, 0x74, 0x41, 0x75, 0x74, 0x68, 0x65, 0x6e, 0x74, 0x69,
	0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x43, 0x6f, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x74, 0x73, 0x12,
	0x41, 0x2e, 0x69, 0x6f, 0x2e, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x62, 0x61, 0x63, 0x6b, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x67,
	0x72, 0x70, 0x63, 0x2e, 0x41, 0x75, 0x74, 0x68, 0x65, 0x6e, 0x74, 0x69, 0x63, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x43, 0x6f, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x74, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x42, 0x2e, 0x69, 0x6f, 0x2e, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65,
	0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x62, 0x61, 0x63, 0x6b, 0x70, 0x6c, 0x61, 0x6e,
	0x65, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x41, 0x75, 0x74, 0x68, 0x65, 0x6e, 0x74, 0x69, 0x63,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x43, 0x6f, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x74, 0x73, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0xa2, 0x01, 0x0a, 0x19, 0x47, 0x65, 0x74,
	0x43, 0x6f, 0x6e, 0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x43, 0x6f, 0x6e,
	0x73, 0x74, 0x61, 0x6e, 0x74, 0x73, 0x12, 0x40, 0x2e, 0x69, 0x6f, 0x2e, 0x64, 0x65, 0x65, 0x70,
	0x68, 0x61, 0x76, 0x65, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x62, 0x61, 0x63, 0x6b,
	0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x43, 0x6f, 0x6e, 0x66, 0x69,
	0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x43, 0x6f, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x74,
	0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x41, 0x2e, 0x69, 0x6f, 0x2e, 0x64, 0x65,
	0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x62, 0x61,
	0x63, 0x6b, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x43, 0x6f, 0x6e,
	0x66, 0x69, 0x67, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x43, 0x6f, 0x6e, 0x73, 0x74, 0x61,
	0x6e, 0x74, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x42, 0x48,
	0x01, 0x50, 0x01, 0x5a, 0x3c, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f,
	0x64, 0x65, 0x65, 0x70, 0x68, 0x61, 0x76, 0x65, 0x6e, 0x2f, 0x64, 0x65, 0x65, 0x70, 0x68, 0x61,
	0x76, 0x65, 0x6e, 0x2d, 0x63, 0x6f, 0x72, 0x65, 0x2f, 0x67, 0x6f, 0x2f, 0x69, 0x6e, 0x74, 0x65,
	0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f, 0x6e, 0x66, 0x69,
	0x67, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_deephaven_proto_config_proto_rawDescOnce sync.Once
	file_deephaven_proto_config_proto_rawDescData = file_deephaven_proto_config_proto_rawDesc
)

func file_deephaven_proto_config_proto_rawDescGZIP() []byte {
	file_deephaven_proto_config_proto_rawDescOnce.Do(func() {
		file_deephaven_proto_config_proto_rawDescData = protoimpl.X.CompressGZIP(file_deephaven_proto_config_proto_rawDescData)
	})
	return file_deephaven_proto_config_proto_rawDescData
}

var file_deephaven_proto_config_proto_msgTypes = make([]protoimpl.MessageInfo, 7)
var file_deephaven_proto_config_proto_goTypes = []interface{}{
	(*AuthenticationConstantsRequest)(nil),  // 0: io.deephaven.proto.backplane.grpc.AuthenticationConstantsRequest
	(*ConfigurationConstantsRequest)(nil),   // 1: io.deephaven.proto.backplane.grpc.ConfigurationConstantsRequest
	(*AuthenticationConstantsResponse)(nil), // 2: io.deephaven.proto.backplane.grpc.AuthenticationConstantsResponse
	(*ConfigurationConstantsResponse)(nil),  // 3: io.deephaven.proto.backplane.grpc.ConfigurationConstantsResponse
	(*ConfigValue)(nil),                     // 4: io.deephaven.proto.backplane.grpc.ConfigValue
	nil,                                     // 5: io.deephaven.proto.backplane.grpc.AuthenticationConstantsResponse.ConfigValuesEntry
	nil,                                     // 6: io.deephaven.proto.backplane.grpc.ConfigurationConstantsResponse.ConfigValuesEntry
}
var file_deephaven_proto_config_proto_depIdxs = []int32{
	5, // 0: io.deephaven.proto.backplane.grpc.AuthenticationConstantsResponse.config_values:type_name -> io.deephaven.proto.backplane.grpc.AuthenticationConstantsResponse.ConfigValuesEntry
	6, // 1: io.deephaven.proto.backplane.grpc.ConfigurationConstantsResponse.config_values:type_name -> io.deephaven.proto.backplane.grpc.ConfigurationConstantsResponse.ConfigValuesEntry
	4, // 2: io.deephaven.proto.backplane.grpc.AuthenticationConstantsResponse.ConfigValuesEntry.value:type_name -> io.deephaven.proto.backplane.grpc.ConfigValue
	4, // 3: io.deephaven.proto.backplane.grpc.ConfigurationConstantsResponse.ConfigValuesEntry.value:type_name -> io.deephaven.proto.backplane.grpc.ConfigValue
	0, // 4: io.deephaven.proto.backplane.grpc.ConfigService.GetAuthenticationConstants:input_type -> io.deephaven.proto.backplane.grpc.AuthenticationConstantsRequest
	1, // 5: io.deephaven.proto.backplane.grpc.ConfigService.GetConfigurationConstants:input_type -> io.deephaven.proto.backplane.grpc.ConfigurationConstantsRequest
	2, // 6: io.deephaven.proto.backplane.grpc.ConfigService.GetAuthenticationConstants:output_type -> io.deephaven.proto.backplane.grpc.AuthenticationConstantsResponse
	3, // 7: io.deephaven.proto.backplane.grpc.ConfigService.GetConfigurationConstants:output_type -> io.deephaven.proto.backplane.grpc.ConfigurationConstantsResponse
	6, // [6:8] is the sub-list for method output_type
	4, // [4:6] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_deephaven_proto_config_proto_init() }
func file_deephaven_proto_config_proto_init() {
	if File_deephaven_proto_config_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_deephaven_proto_config_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AuthenticationConstantsRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_deephaven_proto_config_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ConfigurationConstantsRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_deephaven_proto_config_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AuthenticationConstantsResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_deephaven_proto_config_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ConfigurationConstantsResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_deephaven_proto_config_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ConfigValue); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_deephaven_proto_config_proto_msgTypes[4].OneofWrappers = []interface{}{
		(*ConfigValue_StringValue)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_deephaven_proto_config_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   7,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_deephaven_proto_config_proto_goTypes,
		DependencyIndexes: file_deephaven_proto_config_proto_depIdxs,
		MessageInfos:      file_deephaven_proto_config_proto_msgTypes,
	}.Build()
	File_deephaven_proto_config_proto = out.File
	file_deephaven_proto_config_proto_rawDesc = nil
	file_deephaven_proto_config_proto_goTypes = nil
	file_deephaven_proto_config_proto_depIdxs = nil
}
