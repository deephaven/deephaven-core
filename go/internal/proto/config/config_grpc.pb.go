// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.17.3
// source: deephaven/proto/config.proto

package config

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// ConfigServiceClient is the client API for ConfigService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ConfigServiceClient interface {
	GetAuthenticationConstants(ctx context.Context, in *AuthenticationConstantsRequest, opts ...grpc.CallOption) (*AuthenticationConstantsResponse, error)
	GetConfigurationConstants(ctx context.Context, in *ConfigurationConstantsRequest, opts ...grpc.CallOption) (*ConfigurationConstantsResponse, error)
}

type configServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewConfigServiceClient(cc grpc.ClientConnInterface) ConfigServiceClient {
	return &configServiceClient{cc}
}

func (c *configServiceClient) GetAuthenticationConstants(ctx context.Context, in *AuthenticationConstantsRequest, opts ...grpc.CallOption) (*AuthenticationConstantsResponse, error) {
	out := new(AuthenticationConstantsResponse)
	err := c.cc.Invoke(ctx, "/io.deephaven.proto.backplane.grpc.ConfigService/GetAuthenticationConstants", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *configServiceClient) GetConfigurationConstants(ctx context.Context, in *ConfigurationConstantsRequest, opts ...grpc.CallOption) (*ConfigurationConstantsResponse, error) {
	out := new(ConfigurationConstantsResponse)
	err := c.cc.Invoke(ctx, "/io.deephaven.proto.backplane.grpc.ConfigService/GetConfigurationConstants", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ConfigServiceServer is the server API for ConfigService service.
// All implementations must embed UnimplementedConfigServiceServer
// for forward compatibility
type ConfigServiceServer interface {
	GetAuthenticationConstants(context.Context, *AuthenticationConstantsRequest) (*AuthenticationConstantsResponse, error)
	GetConfigurationConstants(context.Context, *ConfigurationConstantsRequest) (*ConfigurationConstantsResponse, error)
	mustEmbedUnimplementedConfigServiceServer()
}

// UnimplementedConfigServiceServer must be embedded to have forward compatible implementations.
type UnimplementedConfigServiceServer struct {
}

func (UnimplementedConfigServiceServer) GetAuthenticationConstants(context.Context, *AuthenticationConstantsRequest) (*AuthenticationConstantsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetAuthenticationConstants not implemented")
}
func (UnimplementedConfigServiceServer) GetConfigurationConstants(context.Context, *ConfigurationConstantsRequest) (*ConfigurationConstantsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetConfigurationConstants not implemented")
}
func (UnimplementedConfigServiceServer) mustEmbedUnimplementedConfigServiceServer() {}

// UnsafeConfigServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ConfigServiceServer will
// result in compilation errors.
type UnsafeConfigServiceServer interface {
	mustEmbedUnimplementedConfigServiceServer()
}

func RegisterConfigServiceServer(s grpc.ServiceRegistrar, srv ConfigServiceServer) {
	s.RegisterService(&ConfigService_ServiceDesc, srv)
}

func _ConfigService_GetAuthenticationConstants_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AuthenticationConstantsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ConfigServiceServer).GetAuthenticationConstants(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.deephaven.proto.backplane.grpc.ConfigService/GetAuthenticationConstants",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ConfigServiceServer).GetAuthenticationConstants(ctx, req.(*AuthenticationConstantsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ConfigService_GetConfigurationConstants_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ConfigurationConstantsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ConfigServiceServer).GetConfigurationConstants(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.deephaven.proto.backplane.grpc.ConfigService/GetConfigurationConstants",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ConfigServiceServer).GetConfigurationConstants(ctx, req.(*ConfigurationConstantsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ConfigService_ServiceDesc is the grpc.ServiceDesc for ConfigService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ConfigService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "io.deephaven.proto.backplane.grpc.ConfigService",
	HandlerType: (*ConfigServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetAuthenticationConstants",
			Handler:    _ConfigService_GetAuthenticationConstants_Handler,
		},
		{
			MethodName: "GetConfigurationConstants",
			Handler:    _ConfigService_GetConfigurationConstants_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "deephaven/proto/config.proto",
}
