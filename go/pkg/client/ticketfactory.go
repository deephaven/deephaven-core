package client

import (
	"sync/atomic"

	ticketpb2 "github.com/deephaven/deephaven-core/go/internal/proto/ticket"
)

//todo doc eveything

type ticketFactory struct {
	id int32
}

func newTicketFactory() ticketFactory {
	return ticketFactory{}
}

// newTicketNum returns a new ticket number that has not been used before.
func (tf *ticketFactory) nextId() int32 {
	nextTicket := atomic.AddInt32(&tf.id, 1)

	if nextTicket <= 0 {
		// If you ever see this panic... what are you doing?
		panic("out of tickets")
	}

	return nextTicket
}

// newTicket returns a new ticket that has not used before.
func (tf *ticketFactory) newTicket() ticketpb2.Ticket {
	id := tf.nextId()
	return tf.makeTicket(id)
}

// makeTicket turns a ticket ID into a ticket.
func (tf *ticketFactory) makeTicket(id int32) ticketpb2.Ticket {
	bytes := []byte{'e', byte(id), byte(id >> 8), byte(id >> 16), byte(id >> 24)}
	return ticketpb2.Ticket{Ticket: bytes}
}
