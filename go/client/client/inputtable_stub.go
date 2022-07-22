package client

import (
	"context"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/flight"
	"github.com/apache/arrow/go/v8/arrow/memory"
	inputtablepb2 "github.com/deephaven/deephaven-core/go/client/internal/proto/inputtable"
	tablepb2 "github.com/deephaven/deephaven-core/go/client/internal/proto/table"
	ticketpb2 "github.com/deephaven/deephaven-core/go/client/internal/proto/ticket"
)

// AppendOnlyInputTable is a handle to an append-only input table on the server.
// The only difference between this handle and a normal table handle is the ability
// to add data to it using AddTable.
type AppendOnlyInputTable struct {
	*TableHandle
}

// KeyBackedInputTable is a handle to a key-backed input table on the server.
// The only difference between this handle and a normal table handle is the ability
// to add and remove data to and from it using AddTable and DeleteTable.
type KeyBackedInputTable struct {
	*TableHandle
}

// inputTableStub wraps gRPC calls for inputtable.proto.
type inputTableStub struct {
	client *Client

	stub inputtablepb2.InputTableServiceClient // The stub for performing inputtable gRPC requests.
}

func newInputTableStub(client *Client) inputTableStub {
	return inputTableStub{client: client, stub: inputtablepb2.NewInputTableServiceClient(client.grpcChannel)}
}

type inputTableKind = tablepb2.CreateInputTableRequest_InputTableKind

// makeInputTableRequestFromSchema is just a shorthand method to construct a CreateInputTableRequest,
// since it's a very verbose process.
func makeInputTableRequestFromSchema(kind *inputTableKind, resultId *ticketpb2.Ticket, schema *arrow.Schema) *tablepb2.CreateInputTableRequest {
	schemaBytes := flight.SerializeSchema(schema, memory.DefaultAllocator)

	def := &tablepb2.CreateInputTableRequest_Schema{Schema: schemaBytes}
	return &tablepb2.CreateInputTableRequest{ResultId: resultId, Definition: def, Kind: kind}
}

// makeInputTableRequestFromSchema is just a shorthand method to construct a CreateInputTableRequest,
// since it's a very verbose process.
func makeInputTableRequestFromTable(kind *inputTableKind, resultId *ticketpb2.Ticket, table *TableHandle) *tablepb2.CreateInputTableRequest {
	def := &tablepb2.CreateInputTableRequest_SourceTableId{SourceTableId: &tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: table.ticket}}}
	return &tablepb2.CreateInputTableRequest{ResultId: resultId, Definition: def, Kind: kind}
}

// NewAppendOnlyInputTableFromSchema creates a new append-only input table with columns according to the provided schema.
func (its *inputTableStub) NewAppendOnlyInputTableFromSchema(ctx context.Context, schema *arrow.Schema) (*AppendOnlyInputTable, error) {
	kind := inputTableKind{Kind: &tablepb2.CreateInputTableRequest_InputTableKind_InMemoryAppendOnly_{
		InMemoryAppendOnly: &tablepb2.CreateInputTableRequest_InputTableKind_InMemoryAppendOnly{},
	}}
	resultId := its.client.ticketFact.newTicket()
	req := makeInputTableRequestFromSchema(&kind, &resultId, schema)
	newTable, err := its.client.tableStub.createInputTable(ctx, req)
	if err != nil {
		return nil, err
	}
	return &AppendOnlyInputTable{TableHandle: newTable}, nil
}

// NewAppendOnlyInputTableFromTable creates a new append-only input table with the same columns as the provided table.
func (its *inputTableStub) NewAppendOnlyInputTableFromTable(ctx context.Context, table *TableHandle) (*AppendOnlyInputTable, error) {
	if !table.IsValid() {
		return nil, ErrInvalidTableHandle
	}
	kind := inputTableKind{Kind: &tablepb2.CreateInputTableRequest_InputTableKind_InMemoryAppendOnly_{
		InMemoryAppendOnly: &tablepb2.CreateInputTableRequest_InputTableKind_InMemoryAppendOnly{},
	}}
	resultId := its.client.ticketFact.newTicket()
	req := makeInputTableRequestFromTable(&kind, &resultId, table)
	newTable, err := its.client.tableStub.createInputTable(ctx, req)
	if err != nil {
		return nil, err
	}
	return &AppendOnlyInputTable{TableHandle: newTable}, nil
}

// NewKeyBackedInputTableFromSchema creates a new key-backed input table with columns according to the provided schema.
// The columns to use as the keys are specified by keyColumns.
func (its *inputTableStub) NewKeyBackedInputTableFromSchema(ctx context.Context, schema *arrow.Schema, keyColumns ...string) (*KeyBackedInputTable, error) {
	kind := inputTableKind{Kind: &tablepb2.CreateInputTableRequest_InputTableKind_InMemoryKeyBacked_{
		InMemoryKeyBacked: &tablepb2.CreateInputTableRequest_InputTableKind_InMemoryKeyBacked{KeyColumns: keyColumns},
	}}
	resultId := its.client.ticketFact.newTicket()
	req := makeInputTableRequestFromSchema(&kind, &resultId, schema)
	newTable, err := its.client.tableStub.createInputTable(ctx, req)
	if err != nil {
		return nil, err
	}
	return &KeyBackedInputTable{TableHandle: newTable}, nil
}

// NewKeyBackedInputTableFromTable creates a new key-backed input table with the same columns as the provided table.
// The columns to use as the keys are specified by keyColumns.
func (its *inputTableStub) NewKeyBackedInputTableFromTable(ctx context.Context, table *TableHandle, keyColumns ...string) (*KeyBackedInputTable, error) {
	if !table.IsValid() {
		return nil, ErrInvalidTableHandle
	}
	kind := inputTableKind{Kind: &tablepb2.CreateInputTableRequest_InputTableKind_InMemoryKeyBacked_{
		InMemoryKeyBacked: &tablepb2.CreateInputTableRequest_InputTableKind_InMemoryKeyBacked{KeyColumns: keyColumns},
	}}
	resultId := its.client.ticketFact.newTicket()
	req := makeInputTableRequestFromTable(&kind, &resultId, table)
	newTable, err := its.client.tableStub.createInputTable(ctx, req)
	if err != nil {
		return nil, err
	}
	return &KeyBackedInputTable{TableHandle: newTable}, nil
}

// AddTable appends data from the given table to the end of this table.
// This will automatically update all tables derived from this one.
func (th *AppendOnlyInputTable) AddTable(ctx context.Context, toAdd *TableHandle) error {
	if !th.IsValid() || !toAdd.IsValid() {
		return ErrInvalidTableHandle
	}
	return th.client.inputTableStub.addTable(ctx, th.TableHandle, toAdd)
}

// AddTable merges the keys from the given table into this table.
// If a key does not exist in the current table, the entire row is added,
// otherwise the new data row replaces the existing key.
// This will automatically update all tables derived from this one.
func (th *KeyBackedInputTable) AddTable(ctx context.Context, toAdd *TableHandle) error {
	if !th.IsValid() || !toAdd.IsValid() {
		return ErrInvalidTableHandle
	}
	return th.client.inputTableStub.addTable(ctx, th.TableHandle, toAdd)
}

// addTable makes the AddTableToInputTable gRPC request.
// See the docs for AddTable on each kind of table for details.
func (its *inputTableStub) addTable(ctx context.Context, inputTable *TableHandle, toAdd *TableHandle) error {
	ctx, err := its.client.withToken(ctx)
	if err != nil {
		return err
	}

	req := inputtablepb2.AddTableRequest{InputTable: inputTable.ticket, TableToAdd: toAdd.ticket}
	_, err = its.stub.AddTableToInputTable(ctx, &req)
	if err != nil {
		return nil
	}

	return nil
}

// DeleteTable deletes the rows with the given keys from this table.
// The provided table must consist only of columns that were specified as key columns in the input table.
// This will automatically update all tables derived from this one.
func (th *KeyBackedInputTable) DeleteTable(ctx context.Context, toDelete *TableHandle) error {
	if !th.IsValid() || !toDelete.IsValid() {
		return ErrInvalidTableHandle
	}
	return th.client.inputTableStub.deleteTable(ctx, th.TableHandle, toDelete)
}

// deleteTable makes the DeleteTableFromInputTable gRPC request.
// See the docs for DeleteTable for details.
func (its *inputTableStub) deleteTable(ctx context.Context, inputTable *TableHandle, toRemove *TableHandle) error {
	ctx, err := its.client.withToken(ctx)
	if err != nil {
		return err
	}

	req := inputtablepb2.DeleteTableRequest{InputTable: inputTable.ticket, TableToRemove: toRemove.ticket}
	_, err = its.stub.DeleteTableFromInputTable(ctx, &req)
	if err != nil {
		return nil
	}

	return nil
}
