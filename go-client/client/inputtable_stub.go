package client

import (
	"context"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/flight"
	"github.com/apache/arrow/go/v8/arrow/memory"
	inputtablepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/inputtable"
	tablepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/table"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"
)

type AppendOnlyInputTable struct {
	TableHandle
}

type KeyBackedInputTable struct {
	TableHandle
}

type inputTableStub struct {
	client *Client

	stub inputtablepb2.InputTableServiceClient
}

func newInputTableStub(client *Client) inputTableStub {
	return inputTableStub{client: client, stub: inputtablepb2.NewInputTableServiceClient(client.grpcChannel)}
}

type inputTableKind = tablepb2.CreateInputTableRequest_InputTableKind

func makeInputTableRequestFromSchema(kind *inputTableKind, resultId *ticketpb2.Ticket, schema *arrow.Schema) *tablepb2.CreateInputTableRequest {
	pool := memory.NewGoAllocator()
	schemaBytes := flight.SerializeSchema(schema, pool)

	def := &tablepb2.CreateInputTableRequest_Schema{Schema: schemaBytes}
	return &tablepb2.CreateInputTableRequest{ResultId: resultId, Definition: def, Kind: kind}
}

func makeInputTableRequestFromTable(kind *inputTableKind, resultId *ticketpb2.Ticket, table *TableHandle) *tablepb2.CreateInputTableRequest {
	def := &tablepb2.CreateInputTableRequest_SourceTableId{SourceTableId: &tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: table.ticket}}}
	return &tablepb2.CreateInputTableRequest{ResultId: resultId, Definition: def, Kind: kind}
}

// Creates a new append-only input table with columns according to the provided schema.
func (its *inputTableStub) NewAppendOnlyInputTableFromSchema(ctx context.Context, schema *arrow.Schema) (*AppendOnlyInputTable, error) {
	kind := inputTableKind{Kind: &tablepb2.CreateInputTableRequest_InputTableKind_InMemoryAppendOnly_{
		InMemoryAppendOnly: &tablepb2.CreateInputTableRequest_InputTableKind_InMemoryAppendOnly{},
	}}
	resultId := its.client.newTicket()
	req := makeInputTableRequestFromSchema(&kind, &resultId, schema)
	table, err := its.client.tableStub.createInputTable(ctx, req)
	if err != nil {
		return nil, err
	}
	return &AppendOnlyInputTable{TableHandle: *table}, nil
}

// Creates a new append-only input table with the same columns as the provided table.
func (its *inputTableStub) NewAppendOnlyInputTableFromTable(ctx context.Context, table *TableHandle) (*AppendOnlyInputTable, error) {
	kind := inputTableKind{Kind: &tablepb2.CreateInputTableRequest_InputTableKind_InMemoryAppendOnly_{
		InMemoryAppendOnly: &tablepb2.CreateInputTableRequest_InputTableKind_InMemoryAppendOnly{},
	}}
	resultId := its.client.newTicket()
	req := makeInputTableRequestFromTable(&kind, &resultId, table)
	table, err := its.client.tableStub.createInputTable(ctx, req)
	if err != nil {
		return nil, err
	}
	return &AppendOnlyInputTable{TableHandle: *table}, nil
}

// Creates a new key-backed input table with columns according to the provided schema.
// The columns to use as the keys are specified by keyColumns.
func (its *inputTableStub) NewKeyBackedInputTableFromSchema(ctx context.Context, schema *arrow.Schema, keyColumns ...string) (*KeyBackedInputTable, error) {
	kind := inputTableKind{Kind: &tablepb2.CreateInputTableRequest_InputTableKind_InMemoryKeyBacked_{
		InMemoryKeyBacked: &tablepb2.CreateInputTableRequest_InputTableKind_InMemoryKeyBacked{KeyColumns: keyColumns},
	}}
	resultId := its.client.newTicket()
	req := makeInputTableRequestFromSchema(&kind, &resultId, schema)
	table, err := its.client.tableStub.createInputTable(ctx, req)
	if err != nil {
		return nil, err
	}
	return &KeyBackedInputTable{TableHandle: *table}, nil
}

// Creates a new key-backed input table with the same columns as the provided table.
// The columns to use as the keys are specified by keyColumns.
func (its *inputTableStub) NewKeyBackedInputTableFromTable(ctx context.Context, table *TableHandle, keyColumns ...string) (*KeyBackedInputTable, error) {
	kind := inputTableKind{Kind: &tablepb2.CreateInputTableRequest_InputTableKind_InMemoryKeyBacked_{
		InMemoryKeyBacked: &tablepb2.CreateInputTableRequest_InputTableKind_InMemoryKeyBacked{KeyColumns: keyColumns},
	}}
	resultId := its.client.newTicket()
	req := makeInputTableRequestFromTable(&kind, &resultId, table)
	table, err := its.client.tableStub.createInputTable(ctx, req)
	if err != nil {
		return nil, err
	}
	return &KeyBackedInputTable{TableHandle: *table}, nil
}

// Appends data from the given table to the end of this table.
// This will automatically update all tables derived from this one.
func (th *AppendOnlyInputTable) AddTable(ctx context.Context, toAdd *TableHandle) error {
	return th.client.inputTableStub.addTable(ctx, &th.TableHandle, toAdd)
}

// Merges the keys from the given table into this table.
// If a key does not exist in the current table, the entire row is added,
// otherwise the new data row replaces the existing key.
// This will automatically update all tables derived from this one.
func (th *KeyBackedInputTable) AddTable(ctx context.Context, toAdd *TableHandle) error {
	return th.client.inputTableStub.addTable(ctx, &th.TableHandle, toAdd)
}

func (its *inputTableStub) addTable(ctx context.Context, inputTable *TableHandle, toAdd *TableHandle) error {
	ctx = its.client.withToken(ctx)

	req := inputtablepb2.AddTableRequest{InputTable: inputTable.ticket, TableToAdd: toAdd.ticket}
	_, err := its.stub.AddTableToInputTable(ctx, &req)
	if err != nil {
		return nil
	}

	return nil
}

// Delets the rows with the given keys from this table.
// The provided table must consist only of columns that were specified as key columns in the input table.
// This will automatically update all tables derived from this one.
func (th *KeyBackedInputTable) DeleteTable(ctx context.Context, toDelete *TableHandle) error {
	return th.client.inputTableStub.deleteTable(ctx, &th.TableHandle, toDelete)
}

func (its *inputTableStub) deleteTable(ctx context.Context, inputTable *TableHandle, toRemove *TableHandle) error {
	ctx = its.client.withToken(ctx)

	req := inputtablepb2.DeleteTableRequest{InputTable: inputTable.ticket, TableToRemove: toRemove.ticket}
	_, err := its.stub.DeleteTableFromInputTable(ctx, &req)
	if err != nil {
		return nil
	}

	return nil
}
