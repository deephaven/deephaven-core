package client

import (
	"context"
	"errors"

	tablepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/table"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"
)

type tableOp interface {
	makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference) tablepb2.BatchTableRequest_Operation
}

type QueryBuilder struct {
	table *TableHandle
	ops   []tableOp
}

func (qb *QueryBuilder) getGrpcOps() ([]*tablepb2.BatchTableRequest_Operation, error) {
	if len(qb.ops) == 0 {
		return nil, errors.New("cannot execute empty query")
	}

	source := &tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: qb.table.ticket}}

	grpcOps := []*tablepb2.BatchTableRequest_Operation{}

	for _, op := range qb.ops[:len(qb.ops)-1] {
		grpcOp := op.makeBatchOp(nil, source)
		grpcOps = append(grpcOps, &grpcOp)
		source = &tablepb2.TableReference{Ref: &tablepb2.TableReference_BatchOffset{BatchOffset: int32(len(grpcOps) - 1)}}
	}

	result := qb.table.client.NewTicket()

	grpcOp := qb.ops[len(qb.ops)-1].makeBatchOp(&result, source)
	grpcOps = append(grpcOps, &grpcOp)
	return grpcOps, nil
}

func (qb *QueryBuilder) Execute(ctx context.Context) (TableHandle, error) {
	ops, err := qb.getGrpcOps()
	if err != nil {
		return TableHandle{}, err
	}

	exportedTables, err := qb.table.client.batch(ctx, ops)
	if err != nil {
		return TableHandle{}, err
	}

	return exportedTables[len(exportedTables)-1], nil
}

func newQueryBuilder(table *TableHandle) QueryBuilder {
	return QueryBuilder{table: table}
}

type DropColumnsOp struct {
	cols []string
}

func (op DropColumnsOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	req := &tablepb2.DropColumnsRequest{ResultId: resultId, SourceId: sourceId, ColumnNames: op.cols}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_DropColumns{DropColumns: req}}
}

func (qb *QueryBuilder) DropColumns(cols []string) *QueryBuilder {
	qb.ops = append(qb.ops, DropColumnsOp{cols: cols})
	return qb
}

type UpdateOp struct {
	formulas []string
}

func (op UpdateOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	req := &tablepb2.SelectOrUpdateRequest{ResultId: resultId, SourceId: sourceId, ColumnSpecs: op.formulas}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_Update{Update: req}}
}

func (qb *QueryBuilder) Update(formulas []string) *QueryBuilder {
	qb.ops = append(qb.ops, UpdateOp{formulas: formulas})
	return qb
}
