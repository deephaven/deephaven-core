package client

import (
	"bytes"
	"context"
	"errors"

	tablepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/table"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"
)

func assert(cond bool, msg string) {
	if !cond {
		panic(msg)
	}
}

type tableOp interface {
	childQueries() []QueryNode

	makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation
}

// A pointer into a Query DAG
type QueryNode struct {
	// -1 refers to the QueryBuilder's base table
	index   int
	builder *QueryBuilder
}

// This is (some subset of) the Query DAG.
type QueryBuilder struct {
	uniqueId int32
	table    *TableHandle
	ops      []tableOp
}

func (qb *QueryBuilder) curRootNode() QueryNode {
	return QueryNode{index: len(qb.ops) - 1, builder: qb}
}

// Every op can be uniquely identified by its QueryBuilder ID and its index within that QueryBuilder.
type opKey struct {
	index     int
	builderId int32
}

type batchBuilder struct {
	client *Client
	nodes  []QueryNode

	// The response is returned in an arbitrary order.
	// So, we have to keep track of what ticket each table gets, so we can unshuffle them.
	nodeOrder []*ticketpb2.Ticket

	// This map keeps track of operators that are already in the list, to avoid duplication.
	// The value is the index into the full operation list of the op's result.
	finishedOps map[opKey]int32

	grpcOps []*tablepb2.BatchTableRequest_Operation
}

// Returns the index of the node found
func (b *batchBuilder) needsExport(opIdx int, builderId int32) (int, bool) {
	for i, node := range b.nodes {
		if node.index == opIdx && node.builder.uniqueId == builderId {
			return i, true
		}
	}
	return 0, false
}

// Returns a handle to the node's output table
func (b *batchBuilder) addGrpcOps(node QueryNode) *tablepb2.TableReference {
	var source *tablepb2.TableReference
	if node.builder.table != nil {
		source = &tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: node.builder.table.ticket}}
	} else {
		source = nil
	}

	for opIdx, op := range node.builder.ops[:node.index+1] {
		// If the op is already in the list, we don't need to do it again.
		key := opKey{index: opIdx, builderId: node.builder.uniqueId}
		if prevIdx, skip := b.finishedOps[key]; skip {
			// So just use the output of the existing occurence.
			source = &tablepb2.TableReference{Ref: &tablepb2.TableReference_BatchOffset{BatchOffset: prevIdx}}
			continue
		}

		var childQueries []*tablepb2.TableReference = nil
		for _, child := range op.childQueries() {
			childRef := b.addGrpcOps(child)
			childQueries = append(childQueries, childRef)
		}

		var resultId *ticketpb2.Ticket = nil
		if node, ok := b.needsExport(opIdx, node.builder.uniqueId); ok {
			t := b.client.NewTicket()
			resultId = &t
			b.nodeOrder[node] = resultId
		}

		grpcOp := op.makeBatchOp(resultId, source, childQueries)
		b.grpcOps = append(b.grpcOps, &grpcOp)

		source = &tablepb2.TableReference{Ref: &tablepb2.TableReference_BatchOffset{BatchOffset: int32(len(b.grpcOps) - 1)}}
		b.finishedOps[key] = int32(len(b.grpcOps)) - 1
	}

	return source
}

func getGrpcOps(client *Client, nodes []QueryNode) ([]*tablepb2.BatchTableRequest_Operation, []*ticketpb2.Ticket, error) {
	builder := batchBuilder{
		client:      client,
		nodes:       nodes,
		nodeOrder:   make([]*ticketpb2.Ticket, len(nodes)),
		finishedOps: make(map[opKey]int32),
		grpcOps:     nil,
	}

	for _, node := range nodes {
		builder.addGrpcOps(node)
	}

	return builder.grpcOps, builder.nodeOrder, nil
}

// TODO: Duplicate entries in `nodes`

func execQuery(client *Client, ctx context.Context, nodes []QueryNode) ([]TableHandle, error) {
	ops, nodeOrder, err := getGrpcOps(client, nodes)
	if err != nil {
		return nil, err
	}

	if len(nodeOrder) != len(nodes) {
		panic("wrong number of entries in nodeOrder")
	}

	exportedTables, err := client.batch(ctx, ops)
	if err != nil {
		return nil, err
	}

	if len(exportedTables) != len(nodes) {
		return nil, errors.New("wrong number of tables in response")
	}

	var output []TableHandle

	for i, ticket := range nodeOrder {
		for _, tbl := range exportedTables {
			if bytes.Equal(tbl.ticket.GetTicket(), ticket.GetTicket()) {
				output = append(output, tbl)
			}
		}

		if i+1 != len(output) {
			panic("ticket didn't match")
		}
	}

	return output, nil
}

func newQueryBuilder(client *Client, table *TableHandle) QueryBuilder {
	return QueryBuilder{uniqueId: client.NewTicketNum(), table: table}
}

type EmptyTableOp struct {
	numRows int64
}

func (op EmptyTableOp) childQueries() []QueryNode {
	return nil
}

func (op EmptyTableOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for EmptyTable")
	assert(sourceId == nil, "non-nil sourceId for EmptyTable")
	req := &tablepb2.EmptyTableRequest{ResultId: resultId, Size: op.numRows}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_EmptyTable{EmptyTable: req}}
}

type TimeTableOp struct {
	period    int64
	startTime int64
}

func (op TimeTableOp) childQueries() []QueryNode {
	return nil
}

func (op TimeTableOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for TimeTable")
	assert(sourceId == nil, "non-nil sourceId for TimeTable")
	req := &tablepb2.TimeTableRequest{ResultId: resultId, PeriodNanos: op.period, StartTimeNanos: op.startTime}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_TimeTable{TimeTable: req}}
}

type DropColumnsOp struct {
	cols []string
}

func (op DropColumnsOp) childQueries() []QueryNode {
	return nil
}

func (op DropColumnsOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for DropColumns")
	assert(sourceId != nil, "nil sourceId for DropColumns")
	req := &tablepb2.DropColumnsRequest{ResultId: resultId, SourceId: sourceId, ColumnNames: op.cols}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_DropColumns{DropColumns: req}}
}

// Removes the columns with the specified names from the table.
func (qb QueryNode) DropColumns(cols ...string) QueryNode {
	qb.builder.ops = append(qb.builder.ops, DropColumnsOp{cols: cols})
	return qb.builder.curRootNode()
}

type UpdateOp struct {
	formulas []string
}

func (op UpdateOp) childQueries() []QueryNode {
	return nil
}

func (op UpdateOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for Update")
	assert(sourceId != nil, "nil sourceId for Update")
	req := &tablepb2.SelectOrUpdateRequest{ResultId: resultId, SourceId: sourceId, ColumnSpecs: op.formulas}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_Update{Update: req}}
}

// Adds additional columns to the table, calculated based on the given formulas
func (qb QueryNode) Update(formulas ...string) QueryNode {
	qb.builder.ops = append(qb.builder.ops, UpdateOp{formulas: formulas})
	return qb.builder.curRootNode()
}

type MergeOp struct {
	children []QueryNode
	sortBy   string
}

func (op MergeOp) childQueries() []QueryNode {
	return op.children
}

func (op MergeOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == len(op.children), "wrong number of children for Merge")
	assert(sourceId != nil, "nil sourceId for Merge")

	var sourceIds []*tablepb2.TableReference
	sourceIds = append(sourceIds, sourceId)
	sourceIds = append(sourceIds, children...)

	req := &tablepb2.MergeTablesRequest{ResultId: resultId, SourceIds: sourceIds, KeyColumn: op.sortBy}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_Merge{Merge: req}}
}

// Combines additional tables into one table by putting their rows on top of each other.
// All tables involved must have the same columns.
// If sortBy is provided, the resulting table will be sorted based on that column.
func (qb QueryNode) Merge(sortBy string, others ...QueryNode) QueryNode {
	qb.builder.ops = append(qb.builder.ops, MergeOp{children: others, sortBy: sortBy})
	return qb.builder.curRootNode()
}
