package session

import (
	"context"

	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"

	"google.golang.org/grpc"
)

type ConnStub interface {
	GrpcChannel() *grpc.ClientConn

	NewTicketNum() int32

	NewTicket() ticketpb2.Ticket

	MakeTicket(id int32) ticketpb2.Ticket

	WithToken(ctx context.Context) context.Context
}
