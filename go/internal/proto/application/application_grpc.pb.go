// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v5.28.1
// source: deephaven/proto/application.proto

package application

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

// ApplicationServiceClient is the client API for ApplicationService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ApplicationServiceClient interface {
	//
	// Request the list of the fields exposed via the worker.
	//
	// - The first received message contains all fields that are currently available
	//   on the worker. None of these fields will be RemovedFields.
	// - Subsequent messages modify the existing state. Fields are identified by
	//   their ticket and may be replaced or removed.
	ListFields(ctx context.Context, in *ListFieldsRequest, opts ...grpc.CallOption) (ApplicationService_ListFieldsClient, error)
}

type applicationServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewApplicationServiceClient(cc grpc.ClientConnInterface) ApplicationServiceClient {
	return &applicationServiceClient{cc}
}

func (c *applicationServiceClient) ListFields(ctx context.Context, in *ListFieldsRequest, opts ...grpc.CallOption) (ApplicationService_ListFieldsClient, error) {
	stream, err := c.cc.NewStream(ctx, &ApplicationService_ServiceDesc.Streams[0], "/io.deephaven.proto.backplane.grpc.ApplicationService/ListFields", opts...)
	if err != nil {
		return nil, err
	}
	x := &applicationServiceListFieldsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type ApplicationService_ListFieldsClient interface {
	Recv() (*FieldsChangeUpdate, error)
	grpc.ClientStream
}

type applicationServiceListFieldsClient struct {
	grpc.ClientStream
}

func (x *applicationServiceListFieldsClient) Recv() (*FieldsChangeUpdate, error) {
	m := new(FieldsChangeUpdate)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// ApplicationServiceServer is the server API for ApplicationService service.
// All implementations must embed UnimplementedApplicationServiceServer
// for forward compatibility
type ApplicationServiceServer interface {
	//
	// Request the list of the fields exposed via the worker.
	//
	// - The first received message contains all fields that are currently available
	//   on the worker. None of these fields will be RemovedFields.
	// - Subsequent messages modify the existing state. Fields are identified by
	//   their ticket and may be replaced or removed.
	ListFields(*ListFieldsRequest, ApplicationService_ListFieldsServer) error
	mustEmbedUnimplementedApplicationServiceServer()
}

// UnimplementedApplicationServiceServer must be embedded to have forward compatible implementations.
type UnimplementedApplicationServiceServer struct {
}

func (UnimplementedApplicationServiceServer) ListFields(*ListFieldsRequest, ApplicationService_ListFieldsServer) error {
	return status.Errorf(codes.Unimplemented, "method ListFields not implemented")
}
func (UnimplementedApplicationServiceServer) mustEmbedUnimplementedApplicationServiceServer() {}

// UnsafeApplicationServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ApplicationServiceServer will
// result in compilation errors.
type UnsafeApplicationServiceServer interface {
	mustEmbedUnimplementedApplicationServiceServer()
}

func RegisterApplicationServiceServer(s grpc.ServiceRegistrar, srv ApplicationServiceServer) {
	s.RegisterService(&ApplicationService_ServiceDesc, srv)
}

func _ApplicationService_ListFields_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ListFieldsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(ApplicationServiceServer).ListFields(m, &applicationServiceListFieldsServer{stream})
}

type ApplicationService_ListFieldsServer interface {
	Send(*FieldsChangeUpdate) error
	grpc.ServerStream
}

type applicationServiceListFieldsServer struct {
	grpc.ServerStream
}

func (x *applicationServiceListFieldsServer) Send(m *FieldsChangeUpdate) error {
	return x.ServerStream.SendMsg(m)
}

// ApplicationService_ServiceDesc is the grpc.ServiceDesc for ApplicationService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ApplicationService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "io.deephaven.proto.backplane.grpc.ApplicationService",
	HandlerType: (*ApplicationServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ListFields",
			Handler:       _ApplicationService_ListFields_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "deephaven/proto/application.proto",
}
