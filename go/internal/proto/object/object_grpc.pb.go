// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.20.3
// source: deephaven/proto/object.proto

package object

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

// ObjectServiceClient is the client API for ObjectService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ObjectServiceClient interface {
	FetchObject(ctx context.Context, in *FetchObjectRequest, opts ...grpc.CallOption) (*FetchObjectResponse, error)
}

type objectServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewObjectServiceClient(cc grpc.ClientConnInterface) ObjectServiceClient {
	return &objectServiceClient{cc}
}

func (c *objectServiceClient) FetchObject(ctx context.Context, in *FetchObjectRequest, opts ...grpc.CallOption) (*FetchObjectResponse, error) {
	out := new(FetchObjectResponse)
	err := c.cc.Invoke(ctx, "/io.deephaven.proto.backplane.grpc.ObjectService/FetchObject", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ObjectServiceServer is the server API for ObjectService service.
// All implementations must embed UnimplementedObjectServiceServer
// for forward compatibility
type ObjectServiceServer interface {
	FetchObject(context.Context, *FetchObjectRequest) (*FetchObjectResponse, error)
	mustEmbedUnimplementedObjectServiceServer()
}

// UnimplementedObjectServiceServer must be embedded to have forward compatible implementations.
type UnimplementedObjectServiceServer struct {
}

func (UnimplementedObjectServiceServer) FetchObject(context.Context, *FetchObjectRequest) (*FetchObjectResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FetchObject not implemented")
}
func (UnimplementedObjectServiceServer) mustEmbedUnimplementedObjectServiceServer() {}

// UnsafeObjectServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ObjectServiceServer will
// result in compilation errors.
type UnsafeObjectServiceServer interface {
	mustEmbedUnimplementedObjectServiceServer()
}

func RegisterObjectServiceServer(s grpc.ServiceRegistrar, srv ObjectServiceServer) {
	s.RegisterService(&ObjectService_ServiceDesc, srv)
}

func _ObjectService_FetchObject_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FetchObjectRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ObjectServiceServer).FetchObject(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.deephaven.proto.backplane.grpc.ObjectService/FetchObject",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ObjectServiceServer).FetchObject(ctx, req.(*FetchObjectRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ObjectService_ServiceDesc is the grpc.ServiceDesc for ObjectService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ObjectService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "io.deephaven.proto.backplane.grpc.ObjectService",
	HandlerType: (*ObjectServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "FetchObject",
			Handler:    _ObjectService_FetchObject_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "deephaven/proto/object.proto",
}
