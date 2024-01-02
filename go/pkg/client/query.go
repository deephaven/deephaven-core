package client

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	tablepb2 "github.com/deephaven/deephaven-core/go/internal/proto/table"
	ticketpb2 "github.com/deephaven/deephaven-core/go/internal/proto/ticket"
)

// A querySubError is an error when creating a table in a batched or serial query.
// Several querySubErrors may be combined into a queryError.
type querySubError struct {
	serverErr error                    // The raw error returned by the server.
	resultId  *tablepb2.TableReference // The result ID of the table which caused the error. Only present for errors from batch execution.
	source    QueryNode                // The query node that caused this error.
}

// newQuerySubError wraps a single batch sub-error to create a single query sub-error.
// The other parameters are used to attach debugging info to the error.
// nodeTickets is a list of the tickets assigned to each exported node, returned by batch().
// opNodes is a map from grpc op list indices to query nodes, created by newQueryError().
// exportNodes is the list of nodes that are exported from the source query.
func newQuerySubError(subError batchSubError, nodeTickets []*ticketpb2.Ticket, opNodes map[int32]QueryNode, exportNodes []QueryNode) querySubError {
	switch tblRef := subError.ResultId.Ref.(type) {
	case *tablepb2.TableReference_Ticket:
		if idx, ok := findTicketOutputIndex(nodeTickets, tblRef.Ticket); ok {
			return querySubError{serverErr: subError.ServerErr, resultId: subError.ResultId, source: exportNodes[idx]}
		} else {
			return querySubError{serverErr: subError.ServerErr, resultId: subError.ResultId}
		}
	case *tablepb2.TableReference_BatchOffset:
		if key, ok := opNodes[tblRef.BatchOffset]; ok {
			return querySubError{serverErr: subError.ServerErr, resultId: subError.ResultId, source: key}
		} else {
			return querySubError{serverErr: subError.ServerErr, resultId: subError.ResultId}
		}
	default:
		panic(fmt.Sprintf("unreachable table reference type %s", tblRef))
	}
}

// A QueryError may be returned by ExecSerial or ExecBatch as the result of an invalid query.
type QueryError struct {
	// Multiple tables in a query might fail,
	// so each failed table gets its own querySubError.
	subErrors []querySubError
}

// newQueryError wraps a batch error with debugging info.
// nodeTickets is a list of the tickets assigned to each exported node, returned by batch().
// nodeOps is a map from grpc op list indices to query nodes, returned by batch().
// exportNodes is the list of nodes that are exported from the source query.
func newQueryError(err batchError, nodeTickets []*ticketpb2.Ticket, nodeOps map[QueryNode]int32, exportNodes []QueryNode) QueryError {
	opNodes := make(map[int32]QueryNode)
	for key, idx := range nodeOps {
		opNodes[idx] = key
	}

	var wrappedSubErrors []querySubError

	for _, subError := range err.subErrors {
		wrappedSubError := newQuerySubError(subError, nodeTickets, opNodes, exportNodes)
		wrappedSubErrors = append(wrappedSubErrors, wrappedSubError)
	}

	return QueryError{subErrors: wrappedSubErrors}
}

// Unwrap returns the first part of the query error.
func (err QueryError) Unwrap() error {
	return err.subErrors[0].serverErr
}

// Error returns detailed information about all of the sub-errors that occured inside this query error.
// Each sub-error is given a pseudo-traceback of the query operations that caused it.
func (err QueryError) Error() string {
	locked := make(map[*queryBuilder]struct{})
	for _, subError := range err.subErrors {
		if subError.source.builder != nil {
			if _, ok := locked[subError.source.builder]; !ok {
				subError.source.builder.opLock.Lock()
				defer subError.source.builder.opLock.Unlock()
				locked[subError.source.builder] = struct{}{}
			}
		}
	}

	var details string

	for _, subError := range err.subErrors {
		details += fmt.Sprintf("msg: %s\n", subError.serverErr)
		if subError.source.builder != nil {
			builder := subError.source.builder

			if builder.table != nil {
				details += fmt.Sprintf("  base table: %s\n", builder.table.ticket)
			}
			details += "  query operations:\n"
			for _, op := range builder.ops[:subError.source.index+1] {
				details += fmt.Sprintf("    %s\n", op)
			}
		} else {
			details += fmt.Sprintf("no source info (ResultId is %s)\n", subError.resultId)
			details += "file a bug report containing your query code and this error at https://github.com/deephaven/deephaven-core/"
		}
	}
	return details
}

// assert is used to report violated invariants that could only possibly occur as a result of a bad algorithm.
// There should be absolutely no way for a user or network/disk/etc problem to ever cause an assert to fail.
func assert(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

type tableOp interface {
	// childQueries returns the nodes that this operation depends on.
	// The children must be processed first before we can process this operation.
	childQueries() []QueryNode

	// makeBatchOp turns a table operation struct into an actual gRPC request operation.
	// The children argument must be the returned table handles from processing each of the childQueries.
	makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation

	// execSerialOp performs a table operation immediately.
	// The children argument must be the returned table handles from processing each of the childQueries.
	// None of the children may be released before this method returns.
	execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error)
}

// A QueryNode is a pointer somewhere into a "query graph".
// A "query graph" is effectively a list of table operations that can be executed all at once.
//
// Table operations on a QueryNode return other QueryNodes.
// Several operations can be chained together to build up an entire query graph,
// which can then be executed using Client.ExecSerial or Client.ExecBatch to turn the QueryNode into a TableHandle.
// See the TableOps example for more details on how to use query-graph table operations.
//
// All QueryNode methods are goroutine-safe.
type QueryNode struct {
	// -1 refers to the queryBuilder's base table
	index   int
	builder *queryBuilder
}

// addOp appends a new operation to the node's underlying builder, and returns a new node referring to the operation.
func (qb QueryNode) addOp(op tableOp) QueryNode {
	qb.builder.opLock.Lock()
	qb.builder.ops = append(qb.builder.ops, op)
	result := qb.builder.curRootNode()
	qb.builder.opLock.Unlock()
	return result
}

// queryBuilder is (some subgraph of) the Query DAG.
type queryBuilder struct {
	table *TableHandle // This can be nil if the first operation creates a new table, e.g. client.EmptyTableQuery

	opLock sync.Mutex
	ops    []tableOp
}

func (qb *queryBuilder) curRootNode() QueryNode {
	return QueryNode{index: len(qb.ops) - 1, builder: qb}
}

// batchBuilder is used to progressively create an entire batch operation.
// batchBuilder methods are not thread-safe, but batchBuilders are never shared between goroutines anyways.
type batchBuilder struct {
	client *Client

	// The list of nodes that were actually requested as part of a query.
	// This list is kept because we need to specifically export these nodes
	// so that the user can get a TableHandle to them.
	nodes []QueryNode

	// The response is returned in an arbitrary order.
	// So, we have to keep track of what ticket each table gets, so we can unshuffle them.
	nodeOrder []*ticketpb2.Ticket

	// This map keeps track of operators that are already in the list, to avoid duplication.
	// The value is the index into the full operation list of the op's result.
	finishedOps map[QueryNode]int32

	// This is a list of all of the operations currently in the batch.
	// This is what will actually end up in the gRPC request.
	grpcOps []*tablepb2.BatchTableRequest_Operation

	// It is difficult to determine what queryBuilders are used ahead of time,
	// so instead we lock queryBuilders as we encounter them.
	// This set contains all the queryBuilders that have been locked so far.
	lockedBuilders map[*queryBuilder]struct{}

	// It is difficult to determine what TableHandles are used ahead of time,
	// so instead we lock handles as we encounter them.
	// This set contains the handles that have been locked so far.
	lockedTables map[*TableHandle]struct{}
}

// needsExport returns the indexes in the export list of the node found
func (b *batchBuilder) needsExport(node QueryNode) []int {
	var indices []int
	for i, n := range b.nodes {
		if n == node {
			indices = append(indices, i)
		}
	}
	return indices
}

// unlockAll unlocks any locks that were acquired while processing nodes,
// i.e. everything in lockedBuilders and lockedTables.
func (b *batchBuilder) unlockAll() {
	for builder := range b.lockedBuilders {
		builder.opLock.Unlock()
	}

	for table := range b.lockedTables {
		table.lock.RUnlock()
	}

}

// addGrpcOps adds any table operations needed by the node to the list, and returns a handle to the node's output table.
func (b *batchBuilder) addGrpcOps(node QueryNode) (*tablepb2.TableReference, error) {
	var source *tablepb2.TableReference

	// If the op is already in the list, we don't need to do it again.
	if prevIdx, skip := b.finishedOps[node]; skip {
		// So just use the output of the existing occurence.
		return &tablepb2.TableReference{Ref: &tablepb2.TableReference_BatchOffset{BatchOffset: prevIdx}}, nil
	}

	if _, ok := b.lockedBuilders[node.builder]; !ok {
		b.lockedBuilders[node.builder] = struct{}{}
		node.builder.opLock.Lock()
	}

	if node.index == -1 {
		if _, ok := b.lockedTables[node.builder.table]; !ok {
			if !node.builder.table.rLockIfValid() {
				return nil, ErrInvalidTableHandle
			}
			b.lockedTables[node.builder.table] = struct{}{}
		}

		if node.builder.table.client != b.client {
			return nil, ErrDifferentClients
		}
	}

	var resultId *ticketpb2.Ticket = nil
	// Duplicate nodes that still need their own tickets
	var extraNodes []int
	if nodes := b.needsExport(node); len(nodes) > 0 {
		t := b.client.ticketFact.newTicket()
		resultId = &t
		b.nodeOrder[nodes[0]] = resultId

		extraNodes = nodes[1:]

		if node.index == -1 {
			// Even this node needs its own FetchTable request, because it's empty.
			sourceId := &tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: node.builder.table.ticket}}
			t := b.client.ticketFact.newTicket()
			resultId = &t
			b.nodeOrder[nodes[0]] = resultId
			req := tablepb2.FetchTableRequest{ResultId: resultId, SourceId: sourceId}
			grpcOp := tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_FetchTable{FetchTable: &req}}
			b.grpcOps = append(b.grpcOps, &grpcOp)
		}
	} else if node.index == -1 {
		// An unexported node can just re-use the existing ticket since we don't have to worry about aliasing.
		return &tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: node.builder.table.ticket}}, nil
	}

	// Now we actually process the node and turn it (and its children) into gRPC operations.
	if node.index != -1 {
		op := node.builder.ops[node.index]

		if _, ok := op.(mergeOp); ok && len(op.childQueries()) == 0 {
			return nil, ErrEmptyMerge
		}

		var childQueries []*tablepb2.TableReference = nil
		for _, child := range op.childQueries() {
			childRef, err := b.addGrpcOps(child)
			if err != nil {
				return nil, err
			}
			childQueries = append(childQueries, childRef)
		}

		grpcOp := op.makeBatchOp(resultId, childQueries)
		b.grpcOps = append(b.grpcOps, &grpcOp)

		b.finishedOps[node] = int32(len(b.grpcOps)) - 1
	}

	// If this node gets exported multiple times, we need to handle that.
	for _, extraNode := range extraNodes {
		sourceId := &tablepb2.TableReference{Ref: &tablepb2.TableReference_BatchOffset{BatchOffset: int32(len(b.grpcOps) - 1)}}
		t := b.client.ticketFact.newTicket()
		resultId = &t
		b.nodeOrder[extraNode] = resultId
		req := tablepb2.FetchTableRequest{ResultId: resultId, SourceId: sourceId}
		grpcOp := tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_FetchTable{FetchTable: &req}}
		b.grpcOps = append(b.grpcOps, &grpcOp)
	}

	source = &tablepb2.TableReference{Ref: &tablepb2.TableReference_BatchOffset{BatchOffset: int32(len(b.grpcOps) - 1)}}
	return source, nil
}

// getGrpcOps turns a set of query nodes into a sequence of batch operations.
// grpcOps is a list of the raw operations that can be used in the grpc request.
// nodeTickets are the tickets that each query node will be referenced by, where each index in nodeTickets matches the same index in the nodes argument.
// nodeTickets is needed so that we can match up the nodes and the tables once the request finishes.
// nodeOps is a mapping from all of the query nodes processed to the index in grpcOps that they generated.
func getGrpcOps(client *Client, nodes []QueryNode) (grpcOps []*tablepb2.BatchTableRequest_Operation, nodeTickets []*ticketpb2.Ticket, nodeOps map[QueryNode]int32, err error) {
	builder := batchBuilder{
		client:         client,
		nodes:          nodes,
		nodeOrder:      make([]*ticketpb2.Ticket, len(nodes)),
		finishedOps:    make(map[QueryNode]int32),
		grpcOps:        nil,
		lockedBuilders: make(map[*queryBuilder]struct{}),
		lockedTables:   make(map[*TableHandle]struct{}),
	}
	defer builder.unlockAll()

	for _, node := range nodes {
		_, err = builder.addGrpcOps(node)
		if err != nil {
			return
		}
	}

	grpcOps = builder.grpcOps
	nodeTickets = builder.nodeOrder
	nodeOps = builder.finishedOps

	return
}

// findTableOutputIndex finds the index in the nodeTickets list of the output ticket.
// If there are no matches or multiple matches, ok is false.
func findTicketOutputIndex(nodeTickets []*ticketpb2.Ticket, tableTicket *ticketpb2.Ticket) (idx int, ok bool) {
	foundMatch := false

	for i, nodeTicket := range nodeTickets {
		if bytes.Equal(nodeTicket.GetTicket(), tableTicket.GetTicket()) {
			if foundMatch {
				// Multiple matches
				return 0, false
			} else {
				foundMatch = true
				idx = i
			}
		}
	}

	// idx was already set in the loop if found
	return idx, foundMatch
}

// execBatch performs the Batch gRPC operation, which performs several table operations in a single request.
// It then wraps the returned tables in TableHandles and returns them in the same order as in nodes.
func execBatch(client *Client, ctx context.Context, nodes []QueryNode) ([]*TableHandle, error) {
	if len(nodes) == 0 {
		return nil, nil
	}

	ops, nodeTickets, nodeOps, err := getGrpcOps(client, nodes)
	if err != nil {
		return nil, err
	}

	if len(nodeTickets) != len(nodes) {
		panic("wrong number of entries in nodeOrder")
	}

	exportedTables, err := client.batch(ctx, ops)
	if err != nil {
		if err, ok := err.(batchError); ok {
			return nil, newQueryError(err, nodeTickets, nodeOps, nodes)
		}

		return nil, err
	}

	// The tables are returned in arbitrary order,
	// so we have to match the tickets in nodeOrder with the ticket for each table
	// in order to determine which one is which and unshuffle them.
	var output []*TableHandle
	for i, ticket := range nodeTickets {
		for _, tbl := range exportedTables {
			if bytes.Equal(tbl.ticket.GetTicket(), ticket.GetTicket()) {
				output = append(output, tbl)
			}
		}

		if i+1 != len(output) {
			panic(fmt.Sprintf("ticket didn't match %s", ticket))
		}
	}

	return output, nil
}

func newQueryBuilder(table *TableHandle) queryBuilder {
	return queryBuilder{table: table}
}

// emptyTableOp is created by client.EmptyTableQuery().
type emptyTableOp struct {
	numRows int64
}

func (op emptyTableOp) childQueries() []QueryNode {
	return nil
}

func (op emptyTableOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for EmptyTable")
	req := &tablepb2.EmptyTableRequest{ResultId: resultId, Size: op.numRows}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_EmptyTable{EmptyTable: req}}
}

func (op emptyTableOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 0, "wrong number of children for EmptyTable")
	return stub.EmptyTable(ctx, op.numRows)
}

func (op emptyTableOp) String() string {
	return fmt.Sprintf("EmptyTable(%d)", op.numRows)
}

// timeTableOp is created by client.TimeTableQuery().
type timeTableOp struct {
	period    time.Duration
	startTime time.Time
}

func (op timeTableOp) childQueries() []QueryNode {
	return nil
}

func (op timeTableOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for TimeTable")
	req := &tablepb2.TimeTableRequest{ResultId: resultId,
		Period:    &tablepb2.TimeTableRequest_PeriodNanos{PeriodNanos: op.period.Nanoseconds()},
		StartTime: &tablepb2.TimeTableRequest_StartTimeNanos{StartTimeNanos: op.startTime.UnixNano()}}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_TimeTable{TimeTable: req}}
}

func (op timeTableOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 0, "wrong number of children for TimeTable")
	return stub.TimeTable(ctx, op.period, op.startTime)
}

func (op timeTableOp) String() string {
	return fmt.Sprintf("TimeTable(%s, %s)", op.period, op.startTime)
}

type dropColumnsOp struct {
	child QueryNode
	cols  []string
}

func (op dropColumnsOp) childQueries() []QueryNode {
	return []QueryNode{op.child}
}

func (op dropColumnsOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 1, "wrong number of children for DropColumns")
	req := &tablepb2.DropColumnsRequest{ResultId: resultId, SourceId: children[0], ColumnNames: op.cols}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_DropColumns{DropColumns: req}}
}

func (op dropColumnsOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 1, "wrong number of children for DropColumns")
	return stub.dropColumns(ctx, children[0], op.cols)
}

func (op dropColumnsOp) String() string {
	return fmt.Sprintf("DropColumns(%#v)", op.cols)
}

// DropColumns creates a table with the same number of rows as the source table but omits any columns included in the arguments.
func (qb QueryNode) DropColumns(cols ...string) QueryNode {
	return qb.addOp(dropColumnsOp{child: qb, cols: cols})
}

type updateOp struct {
	child    QueryNode
	formulas []string
}

func (op updateOp) childQueries() []QueryNode {
	return []QueryNode{op.child}
}

func (op updateOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 1, "wrong number of children for Update")
	req := &tablepb2.SelectOrUpdateRequest{ResultId: resultId, SourceId: children[0], ColumnSpecs: op.formulas}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_Update{Update: req}}
}

func (op updateOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 1, "wrong number of children for Update")
	return stub.update(ctx, children[0], op.formulas)
}

func (op updateOp) String() string {
	return fmt.Sprintf("Update(%#v)", op.formulas)
}

// Update creates a new table containing a new, in-memory column for each argument.
// The returned table also includes all the original columns from the source table.
func (qb QueryNode) Update(formulas ...string) QueryNode {
	return qb.addOp(updateOp{child: qb, formulas: formulas})
}

type lazyUpdateOp struct {
	child    QueryNode
	formulas []string
}

func (op lazyUpdateOp) childQueries() []QueryNode {
	return []QueryNode{op.child}
}

func (op lazyUpdateOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 1, "wrong number of children for LazyUpdate")
	req := &tablepb2.SelectOrUpdateRequest{ResultId: resultId, SourceId: children[0], ColumnSpecs: op.formulas}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_LazyUpdate{LazyUpdate: req}}
}

func (op lazyUpdateOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 1, "wrong number of children for LazyUpdate")
	return stub.lazyUpdate(ctx, children[0], op.formulas)
}

func (op lazyUpdateOp) String() string {
	return fmt.Sprintf("LazyUpdate(%#v)", op.formulas)
}

// LazyUpdate creates a new table containing a new, cached, formula column for each argument.
// The returned table also includes all the original columns from the source table.
func (qb QueryNode) LazyUpdate(formulas ...string) QueryNode {
	return qb.addOp(lazyUpdateOp{child: qb, formulas: formulas})
}

type viewOp struct {
	child    QueryNode
	formulas []string
}

func (op viewOp) childQueries() []QueryNode {
	return []QueryNode{op.child}
}

func (op viewOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 1, "wrong number of children for View")
	req := &tablepb2.SelectOrUpdateRequest{ResultId: resultId, SourceId: children[0], ColumnSpecs: op.formulas}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_View{View: req}}
}

func (op viewOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 1, "wrong number of children for View")
	return stub.view(ctx, children[0], op.formulas)
}

func (op viewOp) String() string {
	return fmt.Sprintf("View(%#v)", op.formulas)
}

// View creates a new formula table that includes one column for each argument.
// When using view, the data being requested is not stored in memory.
// Rather, a formula is stored that is used to recalculate each cell every time it is accessed.
func (qb QueryNode) View(formulas ...string) QueryNode {
	return qb.addOp(viewOp{child: qb, formulas: formulas})
}

type updateViewOp struct {
	child    QueryNode
	formulas []string
}

func (op updateViewOp) childQueries() []QueryNode {
	return []QueryNode{op.child}
}

func (op updateViewOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 1, "wrong number of children for UpdateView")
	req := &tablepb2.SelectOrUpdateRequest{ResultId: resultId, SourceId: children[0], ColumnSpecs: op.formulas}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_UpdateView{UpdateView: req}}
}

func (op updateViewOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 1, "wrong number of children for UpdateView")
	return stub.updateView(ctx, children[0], op.formulas)
}

func (op updateViewOp) String() string {
	return fmt.Sprintf("UpdateView(%#v)", op.formulas)
}

// UpdateView creates a new table containing a new, formula column for each argument.
// When using UpdateView, the new columns are not stored in memory.
// Rather, a formula is stored that is used to recalculate each cell every time it is accessed.
// The returned table also includes all the original columns from the source table.
func (qb QueryNode) UpdateView(formulas ...string) QueryNode {
	return qb.addOp(updateViewOp{child: qb, formulas: formulas})
}

type selectOp struct {
	child    QueryNode
	formulas []string
}

func (op selectOp) childQueries() []QueryNode {
	return []QueryNode{op.child}
}

func (op selectOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 1, "wrong number of children for Select")
	req := &tablepb2.SelectOrUpdateRequest{ResultId: resultId, SourceId: children[0], ColumnSpecs: op.formulas}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_Select{Select: req}}
}

func (op selectOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 1, "wrong number of children for Select")
	return stub.selectTbl(ctx, children[0], op.formulas)
}

func (op selectOp) String() string {
	return fmt.Sprintf("Select(%#v)", op.formulas)
}

// Select creates a new in-memory table that includes one column for each argument.
// Any columns not specified in the arguments will not appear in the resulting table.
func (qb QueryNode) Select(formulas ...string) QueryNode {
	return qb.addOp(selectOp{child: qb, formulas: formulas})
}

type selectDistinctOp struct {
	child QueryNode
	cols  []string
}

func (op selectDistinctOp) childQueries() []QueryNode {
	return []QueryNode{op.child}
}

func (op selectDistinctOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 1, "wrong number of children for SelectDistinct")
	req := &tablepb2.SelectDistinctRequest{ResultId: resultId, SourceId: children[0], ColumnNames: op.cols}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_SelectDistinct{SelectDistinct: req}}
}

func (op selectDistinctOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 1, "wrong number of children for SelectDistinct")
	return stub.selectDistinct(ctx, children[0], op.cols)
}

func (op selectDistinctOp) String() string {
	return fmt.Sprintf("SelectDistinct(%#v)", op.cols)
}

// SelectDistinct creates a new table containing all of the unique values for a set of key columns.
// When SelectDistinct is used on multiple columns, it looks for distinct sets of values in the selected columns.
func (qb QueryNode) SelectDistinct(columnNames ...string) QueryNode {
	return qb.addOp(selectDistinctOp{child: qb, cols: columnNames})
}

// SortColumn is a pair of a column and a direction to sort it by.
type SortColumn struct {
	colName    string
	descending bool
}

// SortAsc specifies that a particular column should be sorted in ascending order
func SortAsc(colName string) SortColumn {
	return SortColumn{colName: colName, descending: false}
}

// SortDsc specifies that a particular column should be sorted in descending order
func SortDsc(colName string) SortColumn {
	return SortColumn{colName: colName, descending: true}
}

type sortOp struct {
	child   QueryNode
	columns []SortColumn
}

func (op sortOp) childQueries() []QueryNode {
	return []QueryNode{op.child}
}

func (op sortOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 1, "wrong number of children for Sort")

	var sorts []*tablepb2.SortDescriptor
	for _, col := range op.columns {
		var dir tablepb2.SortDescriptor_SortDirection
		if col.descending {
			dir = tablepb2.SortDescriptor_DESCENDING
		} else {
			dir = tablepb2.SortDescriptor_ASCENDING
		}

		sort := tablepb2.SortDescriptor{ColumnName: col.colName, IsAbsolute: false, Direction: dir}
		sorts = append(sorts, &sort)
	}

	req := &tablepb2.SortTableRequest{ResultId: resultId, SourceId: children[0], Sorts: sorts}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_Sort{Sort: req}}
}

func (op sortOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 1, "wrong number of children for Sort")
	return stub.sortBy(ctx, children[0], op.columns)
}

func (op sortOp) String() string {
	return fmt.Sprintf("SortBy(%#v)", op.columns)
}

// Sort returns a new table with rows sorted in a smallest to largest order based on the listed column(s).
func (qb QueryNode) Sort(cols ...string) QueryNode {
	var columns []SortColumn
	for _, col := range cols {
		columns = append(columns, SortAsc(col))
	}
	return qb.SortBy(columns...)
}

// Sort returns a new table with rows sorted in the order specified by the listed column(s).
func (qb QueryNode) SortBy(cols ...SortColumn) QueryNode {
	return qb.addOp(sortOp{child: qb, columns: cols})
}

type filterOp struct {
	child   QueryNode
	filters []string
}

func (op filterOp) childQueries() []QueryNode {
	return []QueryNode{op.child}
}

func (op filterOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 1, "wrong number of children for Filter")
	req := &tablepb2.UnstructuredFilterTableRequest{ResultId: resultId, SourceId: children[0], Filters: op.filters}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_UnstructuredFilter{UnstructuredFilter: req}}
}

func (op filterOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 1, "wrong number of children for Filter")
	return stub.where(ctx, children[0], op.filters)
}

func (op filterOp) String() string {
	return fmt.Sprintf("Where(%#v)", op.filters)
}

// Where filters rows of data from the source table.
// It returns a new table with only the rows meeting the filter criteria of the source table.
func (qb QueryNode) Where(filters ...string) QueryNode {
	return qb.addOp(filterOp{child: qb, filters: filters})
}

type headOrTailOp struct {
	child   QueryNode
	numRows int64
	isTail  bool
}

func (op headOrTailOp) childQueries() []QueryNode {
	return []QueryNode{op.child}
}

func (op headOrTailOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 1, "wrong number of children for Head or Tail")
	req := &tablepb2.HeadOrTailRequest{ResultId: resultId, SourceId: children[0], NumRows: op.numRows}

	if op.isTail {
		return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_Tail{Tail: req}}
	} else {
		return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_Head{Head: req}}
	}
}

func (op headOrTailOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 1, "wrong number of children for Head or Tail")
	return stub.headOrTail(ctx, children[0], op.numRows, !op.isTail)
}

func (op headOrTailOp) String() string {
	if op.isTail {
		return fmt.Sprintf("Tail(%d)", op.numRows)
	} else {
		return fmt.Sprintf("Head(%d)", op.numRows)
	}
}

// Head returns a table with a specific number of rows from the beginning of the source table.
func (qb QueryNode) Head(numRows int64) QueryNode {
	return qb.addOp(headOrTailOp{child: qb, numRows: numRows, isTail: false})
}

// Tail returns a table with a specific number of rows from the end of the source table.
func (qb QueryNode) Tail(numRows int64) QueryNode {
	return qb.addOp(headOrTailOp{child: qb, numRows: numRows, isTail: true})
}

type headOrTailByOp struct {
	child   QueryNode
	numRows int64
	by      []string
	isTail  bool
}

func (op headOrTailByOp) childQueries() []QueryNode {
	return []QueryNode{op.child}
}

func (op headOrTailByOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 1, "wrong number of children for HeadBy or TailBy")

	req := &tablepb2.HeadOrTailByRequest{ResultId: resultId, SourceId: children[0], NumRows: op.numRows, GroupByColumnSpecs: op.by}

	if op.isTail {
		return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_TailBy{TailBy: req}}
	} else {
		return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_HeadBy{HeadBy: req}}
	}
}

func (op headOrTailByOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 1, "wrong number of children for HeadBy or TailBy")
	return stub.headOrTailBy(ctx, children[0], op.numRows, op.by, !op.isTail)
}

func (op headOrTailByOp) String() string {
	if op.isTail {
		return fmt.Sprintf("TailBy(%d, %#v)", op.numRows, op.by)
	} else {
		return fmt.Sprintf("HeadBy(%d, %#v)", op.numRows, op.by)
	}
}

// HeadBy returns the first numRows rows for each group.
func (qb QueryNode) HeadBy(numRows int64, columnsToGroupBy ...string) QueryNode {
	return qb.addOp(headOrTailByOp{child: qb, numRows: numRows, by: columnsToGroupBy, isTail: false})
}

// TailBy returns the last numRows rows for each group.
func (qb QueryNode) TailBy(numRows int64, columnsToGroupBy ...string) QueryNode {
	return qb.addOp(headOrTailByOp{child: qb, numRows: numRows, by: columnsToGroupBy, isTail: true})
}

type ungroupOp struct {
	child    QueryNode
	colNames []string
	nullFill bool
}

func (op ungroupOp) childQueries() []QueryNode {
	return []QueryNode{op.child}
}

func (op ungroupOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 1, "wrong number of children for Ungroup")
	req := &tablepb2.UngroupRequest{ResultId: resultId, SourceId: children[0], ColumnsToUngroup: op.colNames, NullFill: op.nullFill}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_Ungroup{Ungroup: req}}
}

func (op ungroupOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 1, "wrong number of children for Ungroup")
	return stub.ungroup(ctx, children[0], op.colNames, op.nullFill)
}

func (op ungroupOp) String() string {
	return fmt.Sprintf("TailBy(%#v, %t)", op.colNames, op.nullFill)
}

// Ungroup ungroups column content. It is the inverse of the GroupBy method.
// Ungroup unwraps columns containing either Deephaven arrays or Java arrays.
// nullFill indicates whether or not missing cells may be filled with null. Set it to true if you are unsure.
func (qb QueryNode) Ungroup(colsToUngroupBy []string, nullFill bool) QueryNode {
	return qb.addOp(ungroupOp{child: qb, colNames: colsToUngroupBy, nullFill: nullFill})
}

type dedicatedAggOp struct {
	child       QueryNode
	colNames    []string
	countColumn string
	kind        tablepb2.ComboAggregateRequest_AggType
}

func (op dedicatedAggOp) childQueries() []QueryNode {
	return []QueryNode{op.child}
}

func (op dedicatedAggOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 1, "wrong number of children for dedicated aggregation")

	var agg tablepb2.ComboAggregateRequest_Aggregate
	if op.kind == tablepb2.ComboAggregateRequest_COUNT && op.countColumn != "" {
		agg = tablepb2.ComboAggregateRequest_Aggregate{Type: op.kind, ColumnName: op.countColumn}
	} else {
		agg = tablepb2.ComboAggregateRequest_Aggregate{Type: op.kind}
	}

	aggs := []*tablepb2.ComboAggregateRequest_Aggregate{&agg}

	req := &tablepb2.ComboAggregateRequest{ResultId: resultId, SourceId: children[0], Aggregates: aggs, GroupByColumns: op.colNames}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_ComboAggregate{ComboAggregate: req}}
}

func (op dedicatedAggOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 1, "wrong number of children for dedicated aggregation")
	return stub.dedicatedAggOp(ctx, children[0], op.colNames, op.countColumn, op.kind)
}

func (op dedicatedAggOp) String() string {
	switch op.kind {
	case tablepb2.ComboAggregateRequest_SUM:
		return fmt.Sprintf("SumBy(%#v)", op.colNames)
	case tablepb2.ComboAggregateRequest_ABS_SUM:
		return fmt.Sprintf("AbsSumBy(%#v)", op.colNames)
	case tablepb2.ComboAggregateRequest_GROUP:
		return fmt.Sprintf("GroupBy(%#v)", op.colNames)
	case tablepb2.ComboAggregateRequest_AVG:
		return fmt.Sprintf("AvgBy(%#v)", op.colNames)
	case tablepb2.ComboAggregateRequest_COUNT:
		if op.colNames != nil {
			return fmt.Sprintf("CountBy(%s, %#v)", op.countColumn, op.colNames)
		} else {
			return fmt.Sprintf("Count(%s)", op.countColumn)
		}
	case tablepb2.ComboAggregateRequest_FIRST:
		return fmt.Sprintf("FirstBy(%#v)", op.colNames)
	case tablepb2.ComboAggregateRequest_LAST:
		return fmt.Sprintf("LastBy(%#v)", op.colNames)
	case tablepb2.ComboAggregateRequest_MIN:
		return fmt.Sprintf("MinBy(%#v)", op.colNames)
	case tablepb2.ComboAggregateRequest_MAX:
		return fmt.Sprintf("MaxBy(%#v)", op.colNames)
	case tablepb2.ComboAggregateRequest_MEDIAN:
		return fmt.Sprintf("MedianBy(%#v)", op.colNames)
	case tablepb2.ComboAggregateRequest_PERCENTILE:
		// There is no PercentileBy method,
		// so something has gone very wrong if we get here.
		panic("invalid aggregation PERCENTILE")
	case tablepb2.ComboAggregateRequest_STD:
		return fmt.Sprintf("StdBy(%#v)", op.colNames)
	case tablepb2.ComboAggregateRequest_VAR:
		return fmt.Sprintf("VarBy(%#v)", op.colNames)
	case tablepb2.ComboAggregateRequest_WEIGHTED_AVG:
		// There is no WeightedAverageBy method,
		// so something has gone very wrong if we get here.
		panic("invalid aggregation WEIGHTED_AVG")
	default:
		// This is an invalid value for the enumeration,
		// so this is really really wrong.
		panic(fmt.Sprintf("invalid aggregation type %d", op.kind))
	}
}

// GroupBy groups column content into arrays.
// Columns not in the aggregation become array-type.
// If no group-by columns are given, the content of each column is grouped into its own array.
func (qb QueryNode) GroupBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_GROUP})
}

// FirstBy returns the first row for each group.
// If no columns are given, only the first row of the table is returned.
func (qb QueryNode) FirstBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_FIRST})
}

// LastBy returns the last row for each group.
// If no columns are given, only the last row of the table is returned.
func (qb QueryNode) LastBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_LAST})
}

// SumBy returns the total sum for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (qb QueryNode) SumBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_SUM})
}

// AbsSumBy returns the total sum of absolute values for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (qb QueryNode) AbsSumBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_ABS_SUM})
}

// AvgBy returns the average (mean) of each non-key column for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (qb QueryNode) AvgBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_AVG})
}

// StdBy returns the sample standard deviation for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
//
// Sample standard deviation is calculated using `Bessel's correction <https://en.wikipedia.org/wiki/Bessel%27s_correction>`_,
// which ensures that the sample variance will be an unbiased estimator of population variance.
func (qb QueryNode) StdBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_STD})
}

// VarBy returns the sample variance for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
//
// Sample variance is calculated using `Bessel's correction <https://en.wikipedia.org/wiki/Bessel%27s_correction>`_,
// which ensures that the sample variance will be an unbiased estimator of population variance.
func (qb QueryNode) VarBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_VAR})
}

// MedianBy returns the median value for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (qb QueryNode) MedianBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_MEDIAN})
}

// MinBy returns the minimum value for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (qb QueryNode) MinBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_MIN})
}

// MaxBy returns the maximum value for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (qb QueryNode) MaxBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_MAX})
}

// CountBy returns the number of rows for each group.
// The count of each group is stored in a new column named after the resultCol argument.
func (qb QueryNode) CountBy(resultCol string, by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, countColumn: resultCol, kind: tablepb2.ComboAggregateRequest_COUNT})
}

// Count counts the number of values in the specified column and returns it as a table with one row and one column.
func (qb QueryNode) Count(col string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, countColumn: col, kind: tablepb2.ComboAggregateRequest_COUNT})
}

// aggPart is a single part of an aggregation, created by the methods on AggBuilder.
type aggPart struct {
	matchPairs []string // usually the columns on which the operation is performed.
	columnName string   // only used for Count and WeightedAvg.
	percentile float64  // only used for Percentile.
	avgMedian  bool     // not actually used, but here in case more aggregation operations are added.
	// whether this is a sum, avg, median, etc.
	kind tablepb2.ComboAggregateRequest_AggType
}

// AggBuilder is the main way to construct aggregations with multiple parts in them.
// Each one of the methods is the same as the corresponding method on a QueryNode.
// The columns to aggregate over are selected in AggBy.
type AggBuilder struct {
	aggs []aggPart
}

func NewAggBuilder() *AggBuilder {
	return &AggBuilder{}
}

func (b *AggBuilder) addAgg(part aggPart) {
	b.aggs = append(b.aggs, part)
}

// Count returns an aggregator that computes the number of elements within an aggregation group.
// The count of each group is stored in a new column named after the col argument.
func (b *AggBuilder) Count(col string) *AggBuilder {
	b.addAgg(aggPart{columnName: col, kind: tablepb2.ComboAggregateRequest_COUNT})
	return b
}

// Sum returns an aggregator that computes the total sum of values, within an aggregation group, for each input column.
// The source columns are specified by cols.
func (b *AggBuilder) Sum(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_SUM})
	return b
}

// Sum creates an aggregator that computes the total sum of absolute values, within an aggregation group, for each input column.
// The source columns are specified by cols.
func (b *AggBuilder) AbsSum(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_ABS_SUM})
	return b
}

// Group creates an aggregator that computes an array of all values within an aggregation group, for each input column.
// The source columns are specified by cols.
func (b *AggBuilder) Group(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_GROUP})
	return b
}

// Avg creates an aggregator that computes the average (mean) of values, within an aggregation group, for each input column.
// The source columns are specified by cols.
func (b *AggBuilder) Avg(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_AVG})
	return b
}

// First creates an aggregator that computes the first value, within an aggregation group, for each input column.
// The source columns are specified by cols.
func (b *AggBuilder) First(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_FIRST})
	return b
}

// Last creates an aggregator that computes the last value, within an aggregation group, for each input column.
// The source columns are specified by cols.
func (b *AggBuilder) Last(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_LAST})
	return b
}

// Min creates an aggregator that computes the minimum value, within an aggregation group, for each input column.
// The source columns are specified by cols.
func (b *AggBuilder) Min(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_MIN})
	return b
}

// Max returns an aggregator that computes the maximum value, within an aggregation group, for each input column.
// The source columns are specified by cols.
func (b *AggBuilder) Max(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_MAX})
	return b
}

// Median creates an aggregator that computes the median value, within an aggregation group, for each input column.
// The source columns are specified by cols.
func (b *AggBuilder) Median(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_MEDIAN})
	return b
}

// Percentile returns an aggregator that computes the designated percentile of values, within an aggregation group, for each input column.
// The source columns are specified by cols.
func (b *AggBuilder) Percentile(percentile float64, cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, percentile: percentile, kind: tablepb2.ComboAggregateRequest_PERCENTILE})
	return b
}

// Std returns an aggregator that computes the sample standard deviation of values, within an aggregation group, for each input column.
// The source columns are specified by cols.
//
// Sample standard deviation is calculated using `Bessel's correction <https://en.wikipedia.org/wiki/Bessel%27s_correction>`_,
// which ensures that the sample variance will be an unbiased estimator of population variance.
func (b *AggBuilder) StdDev(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_STD})
	return b
}

// Var returns an aggregator that computes the sample variance of values, within an aggregation group, for each input column.
// The source columns are specified by cols.
//
// Sample variance is calculated using `Bessel's correction <https://en.wikipedia.org/wiki/Bessel%27s_correction>`_,
// which ensures that the sample variance will be an unbiased estimator of population variance.
func (b *AggBuilder) Variance(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_VAR})
	return b
}

// WeightedAvg returns an aggregator that computes the weighted average of values, within an aggregation group, for each input column.
// The column to weight by is specified by weightCol.
// The source columns are specified by cols.
func (b *AggBuilder) WeightedAvg(weightCol string, cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, columnName: weightCol, kind: tablepb2.ComboAggregateRequest_WEIGHTED_AVG})
	return b
}

type aggByOp struct {
	child    QueryNode
	colNames []string
	aggs     []aggPart
}

func (op aggByOp) childQueries() []QueryNode {
	return []QueryNode{op.child}
}

func (op aggByOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 1, "wrong number of children for AggBy")

	var aggs []*tablepb2.ComboAggregateRequest_Aggregate
	for _, agg := range op.aggs {
		reqAgg := tablepb2.ComboAggregateRequest_Aggregate{Type: agg.kind, ColumnName: agg.columnName, MatchPairs: agg.matchPairs, Percentile: agg.percentile, AvgMedian: agg.avgMedian}
		aggs = append(aggs, &reqAgg)
	}

	req := &tablepb2.ComboAggregateRequest{ResultId: resultId, SourceId: children[0], Aggregates: aggs, GroupByColumns: op.colNames}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_ComboAggregate{ComboAggregate: req}}
}

func (op aggByOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 1, "wrong number of children for AggBy")
	return stub.aggBy(ctx, children[0], op.aggs, op.colNames)
}

func (op aggByOp) String() string {
	return fmt.Sprintf("AggBy(/* aggregation omitted */, %#v)", op.colNames)
}

// AggBy applies a list of aggregations to table data.
// See the docs on AggBuilder for details on what each of the aggregation types do.
func (qb QueryNode) AggBy(agg *AggBuilder, columnsToGroupBy ...string) QueryNode {
	aggs := make([]aggPart, len(agg.aggs))
	copy(aggs, agg.aggs)
	return qb.addOp(aggByOp{child: qb, colNames: columnsToGroupBy, aggs: aggs})
}

// joinOpKind specifies whether a joinOp is a cross join, natural join, or exact join.
type joinOpKind int

const (
	joinOpCross joinOpKind = iota
	joinOpNatural
	joinOpExact
)

// joinOp can be either a cross join, natural join, or exact join. This is determined by the kind field.
type joinOp struct {
	leftTable      QueryNode
	rightTable     QueryNode
	columnsToMatch []string
	columnsToAdd   []string
	reserveBits    int32 // only used if kind is joinOpCross
	kind           joinOpKind
}

func (op joinOp) childQueries() []QueryNode {
	return []QueryNode{op.leftTable, op.rightTable}
}

func (op joinOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 2, "wrong number of children for CrossJoin, NaturalJoin, or ExactJoin")

	leftId := children[0]
	rightId := children[1]

	switch op.kind {
	case joinOpCross:
		req := &tablepb2.CrossJoinTablesRequest{ResultId: resultId, LeftId: leftId, RightId: rightId, ColumnsToMatch: op.columnsToMatch, ColumnsToAdd: op.columnsToAdd, ReserveBits: op.reserveBits}
		return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_CrossJoin{CrossJoin: req}}
	case joinOpNatural:
		req := &tablepb2.NaturalJoinTablesRequest{ResultId: resultId, LeftId: leftId, RightId: rightId, ColumnsToMatch: op.columnsToMatch, ColumnsToAdd: op.columnsToAdd}
		return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_NaturalJoin{NaturalJoin: req}}
	case joinOpExact:
		req := &tablepb2.ExactJoinTablesRequest{ResultId: resultId, LeftId: leftId, RightId: rightId, ColumnsToMatch: op.columnsToMatch, ColumnsToAdd: op.columnsToAdd}
		return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_ExactJoin{ExactJoin: req}}
	default:
		panic("invalid join kind")
	}
}

func (op joinOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 2, "wrong number of children for CrossJoin, NaturalJoin, or ExactJoin")

	leftTbl := children[0]
	rightTbl := children[1]

	switch op.kind {
	case joinOpCross:
		return stub.crossJoin(ctx, leftTbl, rightTbl, op.columnsToMatch, op.columnsToAdd, op.reserveBits)
	case joinOpNatural:
		return stub.naturalJoin(ctx, leftTbl, rightTbl, op.columnsToMatch, op.columnsToAdd)
	case joinOpExact:
		return stub.exactJoin(ctx, leftTbl, rightTbl, op.columnsToMatch, op.columnsToAdd)
	default:
		panic("invalid join kind")
	}
}

func (op joinOp) String() string {
	switch op.kind {
	case joinOpCross:
		return fmt.Sprintf("Join(/* table omitted */, %#v, %#v, %d)", op.columnsToMatch, op.columnsToAdd, op.reserveBits)
	case joinOpNatural:
		return fmt.Sprintf("NaturalJoin(/* table omitted */, %#v, %#v)", op.columnsToMatch, op.columnsToAdd)
	case joinOpExact:
		return fmt.Sprintf("ExactJoin(/* table omitted */, %#v, %#v", op.columnsToMatch, op.columnsToAdd)
	default:
		panic("unreachable")
	}
}

// Join joins data from a pair of tables - a left and right table - based upon a set of match columns.
// The match columns establish key identifiers in the left table that will be used to find data in the right table.
// Any data types can be chosen as keys, and keys can be constructed from multiple values.
//
// The output table contains rows that have matching values in both tables.
// Rows that do not have matching criteria will not be included in the result.
// If there are multiple matches between a row from the left table and rows from the right table, all matching combinations will be included.
// If no match columns are specified, every combination of left and right table rows is included.
//
// matchOn is the columns to match.
//
// joins is the columns to add from the right table.
//
// reserveBits is the number of bits of key-space to initially reserve per group. Set it to 10 if you are unsure.
func (qb QueryNode) Join(rightTable QueryNode, matchOn []string, joins []string, reserveBits int32) QueryNode {
	op := joinOp{
		leftTable:      qb,
		rightTable:     rightTable,
		columnsToMatch: matchOn,
		columnsToAdd:   joins,
		reserveBits:    reserveBits,
		kind:           joinOpCross,
	}
	return qb.addOp(op)
}

// ExactJoin joins data from a pair of tables - a left and right table - based upon a set of match columns.
// The match columns establish key identifiers in the left table that will be used to find data in the right table.
// Any data types can be chosen as keys, and keys can be constructed from multiple values.
//
// The output table contains all of the rows and columns of the left table plus additional columns containing data from the right table.
// For columns appended to the left table, row values equal the row values from the right table where the key values in the left and right tables are equal.
// If there are zero or multiple matches, the operation will fail.
//
// matchOn is the columns to match.
//
// joins is the columns to add from the right table.
func (qb QueryNode) ExactJoin(rightTable QueryNode, matchOn []string, joins []string) QueryNode {
	op := joinOp{
		leftTable:      qb,
		rightTable:     rightTable,
		columnsToMatch: matchOn,
		columnsToAdd:   joins,
		kind:           joinOpExact,
	}
	return qb.addOp(op)
}

// NaturalJoin joins data from a pair of tables - a left and right table - based upon one or more match columns.
// The match columns establish key identifiers in the left table that will be used to find data in the right table.
// Any data types can be chosen as keys.
//
// The output table contains all of the rows and columns of the left table plus additional columns containing data from the right table.
// For columns appended to the left table, row values equal the row values from the right table where the key values in the left and right tables are equal.
// If there is no matching key in the right table, appended row values are NULL. If there are multiple matches, the operation will fail.
//
// matchOn is the columns to match.
//
// joins is the columns to add from the right table.
func (qb QueryNode) NaturalJoin(rightTable QueryNode, matchOn []string, joins []string) QueryNode {
	op := joinOp{
		leftTable:      qb,
		rightTable:     rightTable,
		columnsToMatch: matchOn,
		columnsToAdd:   joins,
		kind:           joinOpNatural,
	}
	return qb.addOp(op)
}

// A comparison rule for use with AsOfJoin.
// See its documentation for more details.
type MatchRule int

const (
	MatchRuleLessThanEqual MatchRule = iota // Less-than-or-equal, used for an as-of join.
	MatchRuleLessThan
	MatchRuleGreaterThanEqual // Greater-than-or-equal, used for a reverse as-of join.
	MatchRuleGreaterThan
)

// toGrpcMatchRule converts one of the user-friendly match rule constants to the corresponding gRPC match rule enum variants.
func (mr MatchRule) toGrpcMatchRule() tablepb2.AsOfJoinTablesRequest_MatchRule {
	switch mr {
	case MatchRuleLessThanEqual:
		return tablepb2.AsOfJoinTablesRequest_LESS_THAN_EQUAL
	case MatchRuleLessThan:
		return tablepb2.AsOfJoinTablesRequest_LESS_THAN
	case MatchRuleGreaterThanEqual:
		return tablepb2.AsOfJoinTablesRequest_GREATER_THAN_EQUAL
	case MatchRuleGreaterThan:
		return tablepb2.AsOfJoinTablesRequest_GREATER_THAN
	default:
		panic("invalid match rule")
	}
}

func (mr MatchRule) String() string {
	if mr < 0 || mr >= 4 {
		return "invalid match rule"
	} else {
		names := []string{"MatchRuleLessThanEqual", "MatchRuleLessThan", "MatchRuleGreaterThanEqual", "MatchRuleGreaterThan"}
		return names[mr]
	}
}

type asOfJoinOp struct {
	leftTable      QueryNode
	rightTable     QueryNode
	columnsToMatch []string
	columnsToAdd   []string
	matchRule      MatchRule
}

func (op asOfJoinOp) childQueries() []QueryNode {
	return []QueryNode{op.leftTable, op.rightTable}
}

func (op asOfJoinOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 2, "wrong number of children for AsOfJoin")

	leftId := children[0]
	rightId := children[1]
	matchRule := op.matchRule.toGrpcMatchRule()

	req := &tablepb2.AsOfJoinTablesRequest{ResultId: resultId, LeftId: leftId, RightId: rightId, ColumnsToMatch: op.columnsToMatch, ColumnsToAdd: op.columnsToAdd, AsOfMatchRule: matchRule}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_AsOfJoin{AsOfJoin: req}}
}

func (op asOfJoinOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == 2, "wrong number of children for AsOfJoin")

	leftTbl := children[0]
	rightTbl := children[1]

	return stub.asOfJoin(ctx, leftTbl, rightTbl, op.columnsToMatch, op.columnsToAdd, op.matchRule)
}

func (op asOfJoinOp) String() string {
	return fmt.Sprintf("AsOfJoin(/* table omitted */, %#v, %#v, %s)", op.columnsToMatch, op.columnsToAdd, op.matchRule)
}

// AsOfJoin joins data from a pair of tables - a left and right table - based upon one or more match columns.
// The match columns establish key identifiers in the left table that will be used to find data in the right table.
// Any data types can be chosen as keys.
//
// When using AsOfJoin, the first N-1 match columns are exactly matched.
// The last match column is used to find the key values from the right table that are closest to the values in the left table without going over the left value.
// For example, when using MatchRuleLessThanEqual,
// if the right table contains a value 5 and the left table contains values 4 and 6, the right table's 5 will be matched on the left table's 6.
//
// The output table contains all of the rows and columns of the left table plus additional columns containing data from the right table.
// For columns optionally appended to the left table, row values equal the row values from the right table where the keys from the left table most closely match the keys from the right table, as defined above.
// If there is no matching key in the right table, appended row values are NULL.
//
// matchColumns is the columns to match.
//
// joins is the columns to add from the right table.
//
// matchRule is the match rule for the join.
// Use MatchRuleLessThanEqual for a normal as-of join, or MatchRuleGreaterThanEqual for a reverse-as-of-join.
func (qb QueryNode) AsOfJoin(rightTable QueryNode, matchColumns []string, joins []string, matchRule MatchRule) QueryNode {
	op := asOfJoinOp{
		leftTable:      qb,
		rightTable:     rightTable,
		columnsToMatch: matchColumns,
		columnsToAdd:   joins,
		matchRule:      matchRule,
	}
	return qb.addOp(op)
}

type mergeOp struct {
	children []QueryNode
	sortBy   string
}

func (op mergeOp) childQueries() []QueryNode {
	return op.children
}

func (op mergeOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == len(op.children), "wrong number of children for Merge")
	assert(len(children) != 0, "cannot make an empty Merge into a batch operation")

	req := &tablepb2.MergeTablesRequest{ResultId: resultId, SourceIds: children, KeyColumn: op.sortBy}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_Merge{Merge: req}}
}

func (op mergeOp) execSerialOp(ctx context.Context, stub *tableStub, children []*TableHandle) (*TableHandle, error) {
	assert(len(children) == len(op.children), "wrong number of children for Merge")

	if len(children) == 0 {
		return nil, ErrEmptyMerge
	}

	return stub.merge(ctx, op.sortBy, children)
}

func (op mergeOp) String() string {
	return fmt.Sprintf("MergeQuery(%s, /* %d tables omitted */)", op.sortBy, len(op.children))
}

// MergeQuery combines two or more tables into one table as part of a query.
// This essentially appends the tables on top of each other.
//
// If sortBy is provided, the resulting table will be sorted based on that column.
//
// "nil Table" nodes (i.e. QueryNodes returned from calling Query() on a nil *TableHandle) are ignored.
//
// At least one non-nil query node must be provided, otherwise an ErrEmptyMerge will be returned by ExecSerial or ExecBatch.
func MergeQuery(sortBy string, tables ...QueryNode) QueryNode {
	var nonNilTables []QueryNode
	for _, table := range tables {
		if table.index != -1 || table.builder.table != nil {
			nonNilTables = append(nonNilTables, table)
		}
	}

	if len(nonNilTables) == 0 {
		// A merge with no tables will always error,
		// but since we can't return an error from this method
		// we have to let ExecSerial or ExecBatch handle the error.
		emptyNode := QueryNode{builder: &queryBuilder{}}
		return emptyNode.addOp(mergeOp{children: tables, sortBy: sortBy})
	}

	// Just pick an arbitrary node to add the operation to.
	qb := nonNilTables[0]

	return qb.addOp(mergeOp{children: nonNilTables, sortBy: sortBy})
}
