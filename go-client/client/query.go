package client

import (
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

func getGrpcOps(client *Client, nodes []QueryNode) ([]*tablepb2.BatchTableRequest_Operation, error) {
	// This map keeps track of operators that are already in the list, to avoid duplication.
	// The value is the index into the full operation list of the op's result.
	finishedOps := make(map[opKey]int32)

	needsExport := func(opIdx int, builderId int32) bool {
		for _, node := range nodes {
			if node.index == opIdx && node.builder.uniqueId == builderId {
				return true
			}
		}
		return false
	}

	grpcOps := []*tablepb2.BatchTableRequest_Operation{}

	for _, node := range nodes {
		if len(node.builder.ops) == 0 {
			return nil, errors.New("cannot execute query with empty builder")
		}

		var source *tablepb2.TableReference
		if node.builder.table != nil {
			source = &tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: node.builder.table.ticket}}
		} else {
			source = nil
		}

		for opIdx, op := range node.builder.ops[:node.index+1] {
			// If the op is already in the list, we don't need to do it again.
			key := opKey{index: opIdx, builderId: node.builder.uniqueId}
			if prevIdx, skip := finishedOps[key]; skip {
				// So just use the output of the existing occurence.
				source = &tablepb2.TableReference{Ref: &tablepb2.TableReference_BatchOffset{BatchOffset: prevIdx}}
				continue
			}

			var childQueries []*tablepb2.TableReference = nil
			if len(op.childQueries()) != 0 {
				// childQueries = something
				panic("TODO: Child queries!!!")
			}

			var resultId *ticketpb2.Ticket = nil
			if needsExport(opIdx, node.builder.uniqueId) {
				t := client.NewTicket()
				resultId = &t
			}

			grpcOp := op.makeBatchOp(resultId, source, childQueries)
			grpcOps = append(grpcOps, &grpcOp)

			source = &tablepb2.TableReference{Ref: &tablepb2.TableReference_BatchOffset{BatchOffset: int32(len(grpcOps) - 1)}}
			finishedOps[key] = int32(len(grpcOps)) - 1
		}
	}

	return grpcOps, nil
}

func execQuery(client *Client, ctx context.Context, nodes []QueryNode) ([]TableHandle, error) {
	ops, err := getGrpcOps(client, nodes)
	if err != nil {
		return nil, err
	}

	exportedTables, err := client.batch(ctx, ops)
	if err != nil {
		return nil, err
	}

	return exportedTables, nil
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

func (qb QueryNode) DropColumns(cols []string) QueryNode {
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

func (qb QueryNode) Update(formulas []string) QueryNode {
	qb.builder.ops = append(qb.builder.ops, UpdateOp{formulas: formulas})
	return qb.builder.curRootNode()
}
