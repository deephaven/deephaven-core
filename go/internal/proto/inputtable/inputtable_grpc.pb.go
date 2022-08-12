// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.17.3
// source: deephaven/proto/inputtable.proto

package inputtable

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

// InputTableServiceClient is the client API for InputTableService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type InputTableServiceClient interface {
	// Adds the provided table to the specified input table. The new data to add must only have
	// columns (name, types, and order) which match the given input table's columns.
	AddTableToInputTable(ctx context.Context, in *AddTableRequest, opts ...grpc.CallOption) (*AddTableResponse, error)
	// Removes the provided table from the specified input tables. The tables indicating which rows
	// to remove are expected to only have columns that match the key columns of the input table.
	DeleteTableFromInputTable(ctx context.Context, in *DeleteTableRequest, opts ...grpc.CallOption) (*DeleteTableResponse, error)
}

type inputTableServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewInputTableServiceClient(cc grpc.ClientConnInterface) InputTableServiceClient {
	return &inputTableServiceClient{cc}
}

func (c *inputTableServiceClient) AddTableToInputTable(ctx context.Context, in *AddTableRequest, opts ...grpc.CallOption) (*AddTableResponse, error) {
	out := new(AddTableResponse)
	err := c.cc.Invoke(ctx, "/io.deephaven.proto.backplane.grpc.InputTableService/AddTableToInputTable", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *inputTableServiceClient) DeleteTableFromInputTable(ctx context.Context, in *DeleteTableRequest, opts ...grpc.CallOption) (*DeleteTableResponse, error) {
	out := new(DeleteTableResponse)
	err := c.cc.Invoke(ctx, "/io.deephaven.proto.backplane.grpc.InputTableService/DeleteTableFromInputTable", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// InputTableServiceServer is the server API for InputTableService service.
// All implementations must embed UnimplementedInputTableServiceServer
// for forward compatibility
type InputTableServiceServer interface {
	// Adds the provided table to the specified input table. The new data to add must only have
	// columns (name, types, and order) which match the given input table's columns.
	AddTableToInputTable(context.Context, *AddTableRequest) (*AddTableResponse, error)
	// Removes the provided table from the specified input tables. The tables indicating which rows
	// to remove are expected to only have columns that match the key columns of the input table.
	DeleteTableFromInputTable(context.Context, *DeleteTableRequest) (*DeleteTableResponse, error)
	mustEmbedUnimplementedInputTableServiceServer()
}

// UnimplementedInputTableServiceServer must be embedded to have forward compatible implementations.
type UnimplementedInputTableServiceServer struct {
}

func (UnimplementedInputTableServiceServer) AddTableToInputTable(context.Context, *AddTableRequest) (*AddTableResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddTableToInputTable not implemented")
}
func (UnimplementedInputTableServiceServer) DeleteTableFromInputTable(context.Context, *DeleteTableRequest) (*DeleteTableResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteTableFromInputTable not implemented")
}
func (UnimplementedInputTableServiceServer) mustEmbedUnimplementedInputTableServiceServer() {}

// UnsafeInputTableServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to InputTableServiceServer will
// result in compilation errors.
type UnsafeInputTableServiceServer interface {
	mustEmbedUnimplementedInputTableServiceServer()
}

func RegisterInputTableServiceServer(s grpc.ServiceRegistrar, srv InputTableServiceServer) {
	s.RegisterService(&InputTableService_ServiceDesc, srv)
}

func _InputTableService_AddTableToInputTable_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddTableRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InputTableServiceServer).AddTableToInputTable(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.deephaven.proto.backplane.grpc.InputTableService/AddTableToInputTable",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InputTableServiceServer).AddTableToInputTable(ctx, req.(*AddTableRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _InputTableService_DeleteTableFromInputTable_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteTableRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InputTableServiceServer).DeleteTableFromInputTable(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/io.deephaven.proto.backplane.grpc.InputTableService/DeleteTableFromInputTable",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InputTableServiceServer).DeleteTableFromInputTable(ctx, req.(*DeleteTableRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// InputTableService_ServiceDesc is the grpc.ServiceDesc for InputTableService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var InputTableService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "io.deephaven.proto.backplane.grpc.InputTableService",
	HandlerType: (*InputTableServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "AddTableToInputTable",
			Handler:    _InputTableService_AddTableToInputTable_Handler,
		},
		{
			MethodName: "DeleteTableFromInputTable",
			Handler:    _InputTableService_DeleteTableFromInputTable_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "deephaven/proto/inputtable.proto",
}
