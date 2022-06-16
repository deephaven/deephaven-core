package client

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/apache/arrow/go/v8/arrow/flight"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"google.golang.org/grpc"

	tablepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/table"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"
)

// May be returned by ExecQuery as the result of an invalid query.
type QueryError struct {
	Msg string
}

func (err QueryError) Error() string {
	return "query error: " + err.Msg
}

type tableStub struct {
	client *Client

	stub tablepb2.TableServiceClient
}

func newTableStub(client *Client) (tableStub, error) {
	stub := tablepb2.NewTableServiceClient(client.grpcChannel)

	return tableStub{client: client, stub: stub}, nil
}

func (ts *tableStub) createInputTable(ctx context.Context, req *tablepb2.CreateInputTableRequest) (*TableHandle, error) {
	ctx = ts.client.withToken(ctx)

	resp, err := ts.stub.CreateInputTable(ctx, req)
	if err != nil {
		return nil, err
	}

	return parseCreationResponse(ts.client, resp)
}

func (ts *tableStub) batch(ctx context.Context, ops []*tablepb2.BatchTableRequest_Operation) ([]*TableHandle, error) {
	ctx = ts.client.withToken(ctx)

	req := tablepb2.BatchTableRequest{Ops: ops}
	resp, err := ts.stub.Batch(ctx, &req)
	if err != nil {
		return nil, err
	}
	defer resp.CloseSend()

	exportedTables := []*TableHandle{}

	for {
		created, err := resp.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		if !created.Success {
			return nil, QueryError{Msg: created.GetErrorInfo()}
		}

		if _, ok := created.ResultId.Ref.(*tablepb2.TableReference_Ticket); ok {
			newTable, err := parseCreationResponse(ts.client, created)
			if err != nil {
				return nil, err
			}
			exportedTables = append(exportedTables, newTable)
		}
	}

	return exportedTables, nil
}

// Opens a globally-scoped table with the given name on the server.
func (ts *tableStub) OpenTable(ctx context.Context, name string) (*TableHandle, error) {
	if ts.client.Closed() {
		return nil, ErrClosedClient
	}

	ctx = ts.client.withToken(ctx)

	fieldId := fieldId{appId: "scope", fieldName: name}
	if tbl, ok := ts.client.tables[fieldId]; ok {
		sourceId := tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: tbl.ticket}}
		resultId := ts.client.newTicket()

		req := tablepb2.FetchTableRequest{SourceId: &sourceId, ResultId: &resultId}
		resp, err := ts.stub.FetchTable(ctx, &req)
		if err != nil {
			return nil, err
		}

		return parseCreationResponse(ts.client, resp)
	} else {
		return nil, errors.New("no table by the name " + name + " (maybe it isn't synced?)")
	}
}

// Like `EmptyTable`, except it can be used as part of a query.
func (ts *tableStub) EmptyTableQuery(numRows int64) QueryNode {
	qb := newQueryBuilder(ts.client, nil)
	qb.ops = append(qb.ops, emptyTableOp{numRows: numRows})
	return qb.curRootNode()
}

// Creates a new empty table in the global scope.
//
// The table will have zero columns and the specified number of rows.
func (ts *tableStub) EmptyTable(ctx context.Context, numRows int64) (*TableHandle, error) {
	if ts.client.Closed() {
		return nil, ErrClosedClient
	}

	ctx = ts.client.withToken(ctx)

	result := ts.client.newTicket()

	req := tablepb2.EmptyTableRequest{ResultId: &result, Size: numRows}
	resp, err := ts.stub.EmptyTable(ctx, &req)
	if err != nil {
		return nil, err
	}

	return parseCreationResponse(ts.client, resp)
}

// Like `TimeTable`, except it can be used as part of a query.
func (ts *tableStub) TimeTableQuery(period int64, startTime *int64) QueryNode {
	var realStartTime int64
	if startTime == nil {
		// TODO: Same question as for TimeTable
		realStartTime = time.Now().UnixNano()
	} else {
		realStartTime = *startTime
	}

	qb := newQueryBuilder(ts.client, nil)
	qb.ops = append(qb.ops, timeTableOp{period: period, startTime: realStartTime})
	return qb.curRootNode()
}

// Creates a ticking time table in the global scope.
// The period is in nanoseconds and represents the interval between adding new rows to the table.
// The startTime is in nanoseconds since epoch and defaults to the current time when it is nil.
func (ts *tableStub) TimeTable(ctx context.Context, period int64, startTime *int64) (*TableHandle, error) {
	if ts.client.Closed() {
		return nil, ErrClosedClient
	}

	ctx = ts.client.withToken(ctx)

	result := ts.client.newTicket()

	var realStartTime int64
	if startTime == nil {
		// TODO: Is this affected by timezones? Does it need to be the monotonic reading?
		realStartTime = time.Now().UnixNano()
	} else {
		realStartTime = *startTime
	}

	req := tablepb2.TimeTableRequest{ResultId: &result, PeriodNanos: period, StartTimeNanos: realStartTime}
	resp, err := ts.stub.TimeTable(ctx, &req)
	if err != nil {
		return nil, err
	}

	return parseCreationResponse(ts.client, resp)
}

func parseCreationResponse(client *Client, resp *tablepb2.ExportedTableCreationResponse) (*TableHandle, error) {
	if !resp.Success {
		return nil, errors.New("server error: `" + resp.GetErrorInfo() + "`")
	}

	respTicket := resp.ResultId.GetTicket()
	if respTicket == nil {
		return nil, errors.New("server response did not have ticket")
	}

	alloc := memory.NewGoAllocator()
	schema, err := flight.DeserializeSchema(resp.SchemaHeader, alloc)
	if err != nil {
		return nil, err
	}

	return newTableHandle(client, respTicket, schema, resp.Size, resp.IsStatic), nil
}

func (ts *tableStub) dropColumns(ctx context.Context, table *TableHandle, cols []string) (*TableHandle, error) {
	if ts.client.Closed() {
		return nil, ErrClosedClient
	}

	ctx = ts.client.withToken(ctx)

	result := ts.client.newTicket()

	source := tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: table.ticket}}

	req := tablepb2.DropColumnsRequest{ResultId: &result, SourceId: &source, ColumnNames: cols}
	resp, err := ts.stub.DropColumns(ctx, &req)
	if err != nil {
		return nil, err
	}

	return parseCreationResponse(ts.client, resp)
}

type selectOrUpdateOp func(tablepb2.TableServiceClient, context.Context, *tablepb2.SelectOrUpdateRequest, ...grpc.CallOption) (*tablepb2.ExportedTableCreationResponse, error)

func (ts *tableStub) doSelectOrUpdate(ctx context.Context, table *TableHandle, formulas []string, op selectOrUpdateOp) (*TableHandle, error) {
	if ts.client.Closed() {
		return nil, ErrClosedClient
	}

	ctx = ts.client.withToken(ctx)

	result := ts.client.newTicket()
	source := tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: table.ticket}}

	req := tablepb2.SelectOrUpdateRequest{ResultId: &result, SourceId: &source, ColumnSpecs: formulas}
	resp, err := op(ts.stub, ctx, &req)
	if err != nil {
		return nil, err
	}

	return parseCreationResponse(ts.client, resp)
}

func (ts *tableStub) update(ctx context.Context, table *TableHandle, formulas []string) (*TableHandle, error) {
	return ts.doSelectOrUpdate(ctx, table, formulas, tablepb2.TableServiceClient.Update)
}

func (ts *tableStub) lazyUpdate(ctx context.Context, table *TableHandle, formulas []string) (*TableHandle, error) {
	return ts.doSelectOrUpdate(ctx, table, formulas, tablepb2.TableServiceClient.LazyUpdate)
}

func (ts *tableStub) updateView(ctx context.Context, table *TableHandle, formulas []string) (*TableHandle, error) {
	return ts.doSelectOrUpdate(ctx, table, formulas, tablepb2.TableServiceClient.UpdateView)
}

func (ts *tableStub) view(ctx context.Context, table *TableHandle, formulas []string) (*TableHandle, error) {
	return ts.doSelectOrUpdate(ctx, table, formulas, tablepb2.TableServiceClient.View)
}

func (ts *tableStub) selectTbl(ctx context.Context, table *TableHandle, formulas []string) (*TableHandle, error) {
	return ts.doSelectOrUpdate(ctx, table, formulas, tablepb2.TableServiceClient.Select)
}

func (ts *tableStub) makeRequest(ctx context.Context, table *TableHandle, op reqOp) (*TableHandle, error) {
	if ts.client.Closed() {
		return nil, ErrClosedClient
	}

	ctx = ts.client.withToken(ctx)

	result := ts.client.newTicket()
	source := tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: table.ticket}}

	resp, err := op(ctx, &result, &source)

	if err != nil {
		return nil, err
	}

	return parseCreationResponse(ts.client, resp)
}

type ctxt = context.Context
type ticketRef = *ticketpb2.Ticket
type tblRef = *tablepb2.TableReference
type tblResp = *tablepb2.ExportedTableCreationResponse

type reqOp func(ctx ctxt, resultId ticketRef, sourceId tblRef) (tblResp, error)

func (ts *tableStub) selectDistinct(ctx context.Context, table *TableHandle, formulas []string) (*TableHandle, error) {
	return ts.makeRequest(ctx, table, func(ctx ctxt, resultId ticketRef, sourceId tblRef) (tblResp, error) {
		req := tablepb2.SelectDistinctRequest{ResultId: resultId, SourceId: sourceId, ColumnNames: formulas}
		return ts.stub.SelectDistinct(ctx, &req)
	})
}

func (ts *tableStub) sortBy(ctx context.Context, table *TableHandle, cols []SortColumn) (*TableHandle, error) {
	return ts.makeRequest(ctx, table, func(ctx ctxt, resultId ticketRef, sourceId tblRef) (tblResp, error) {
		var sorts []*tablepb2.SortDescriptor
		for _, col := range cols {
			var dir tablepb2.SortDescriptor_SortDirection
			if col.descending {
				dir = tablepb2.SortDescriptor_DESCENDING
			} else {
				dir = tablepb2.SortDescriptor_ASCENDING
			}

			sort := tablepb2.SortDescriptor{ColumnName: col.colName, IsAbsolute: false, Direction: dir}
			sorts = append(sorts, &sort)
		}

		req := tablepb2.SortTableRequest{ResultId: resultId, SourceId: sourceId, Sorts: sorts}
		return ts.stub.Sort(ctx, &req)
	})
}

func (ts *tableStub) where(ctx context.Context, table *TableHandle, filters []string) (*TableHandle, error) {
	return ts.makeRequest(ctx, table, func(ctx ctxt, resultId ticketRef, sourceId tblRef) (tblResp, error) {
		req := tablepb2.UnstructuredFilterTableRequest{ResultId: resultId, SourceId: sourceId, Filters: filters}
		return ts.stub.UnstructuredFilter(ctx, &req)
	})
}

func (ts *tableStub) headOrTail(ctx context.Context, table *TableHandle, numRows int64, isHead bool) (*TableHandle, error) {
	return ts.makeRequest(ctx, table, func(ctx ctxt, resultId ticketRef, sourceId tblRef) (tblResp, error) {
		req := tablepb2.HeadOrTailRequest{ResultId: resultId, SourceId: sourceId, NumRows: numRows}
		if isHead {
			return ts.stub.Head(ctx, &req)
		} else {
			return ts.stub.Tail(ctx, &req)
		}
	})
}

func (ts *tableStub) naturalJoin(ctx context.Context, leftTable *TableHandle, rightTable *TableHandle, on []string, joins []string) (*TableHandle, error) {
	return ts.makeRequest(ctx, leftTable, func(ctx ctxt, resultId ticketRef, leftId tblRef) (tblResp, error) {
		rightId := &tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: rightTable.ticket}}
		req := tablepb2.NaturalJoinTablesRequest{ResultId: resultId, LeftId: leftId, RightId: rightId, ColumnsToMatch: on, ColumnsToAdd: joins}
		return ts.stub.NaturalJoinTables(ctx, &req)
	})
}

func (ts *tableStub) crossJoin(ctx context.Context, leftTable *TableHandle, rightTable *TableHandle, on []string, joins []string, reserveBits int32) (*TableHandle, error) {
	return ts.makeRequest(ctx, leftTable, func(ctx ctxt, resultId ticketRef, leftId tblRef) (tblResp, error) {
		rightId := &tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: rightTable.ticket}}
		req := tablepb2.CrossJoinTablesRequest{ResultId: resultId, LeftId: leftId, RightId: rightId, ColumnsToMatch: on, ColumnsToAdd: joins, ReserveBits: reserveBits}
		return ts.stub.CrossJoinTables(ctx, &req)
	})
}

func (ts *tableStub) exactJoin(ctx context.Context, leftTable *TableHandle, rightTable *TableHandle, on []string, joins []string) (*TableHandle, error) {
	return ts.makeRequest(ctx, leftTable, func(ctx ctxt, resultId ticketRef, leftId tblRef) (tblResp, error) {
		rightId := &tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: rightTable.ticket}}
		req := tablepb2.ExactJoinTablesRequest{ResultId: resultId, LeftId: leftId, RightId: rightId, ColumnsToMatch: on, ColumnsToAdd: joins}
		return ts.stub.ExactJoinTables(ctx, &req)
	})
}

func (ts *tableStub) asOfJoin(ctx context.Context, leftTable *TableHandle, rightTable *TableHandle, on []string, joins []string, matchRule int) (*TableHandle, error) {
	return ts.makeRequest(ctx, leftTable, func(ctx ctxt, resultId ticketRef, leftId tblRef) (tblResp, error) {
		rightId := &tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: rightTable.ticket}}
		var asOfMatchRule tablepb2.AsOfJoinTablesRequest_MatchRule
		switch matchRule {
		case MatchRuleLessThanEqual:
			asOfMatchRule = tablepb2.AsOfJoinTablesRequest_LESS_THAN_EQUAL
		case MatchRuleLessThan:
			asOfMatchRule = tablepb2.AsOfJoinTablesRequest_LESS_THAN
		case MatchRuleGreaterThanEqual:
			asOfMatchRule = tablepb2.AsOfJoinTablesRequest_GREATER_THAN_EQUAL
		case MatchRuleGreaterThan:
			asOfMatchRule = tablepb2.AsOfJoinTablesRequest_GREATER_THAN
		default:
			panic("invalid match rule")
		}

		req := tablepb2.AsOfJoinTablesRequest{ResultId: resultId, LeftId: leftId, RightId: rightId, ColumnsToMatch: on, ColumnsToAdd: joins, AsOfMatchRule: asOfMatchRule}
		return ts.stub.AsOfJoinTables(ctx, &req)
	})
}

func (ts *tableStub) headOrTailBy(ctx context.Context, table *TableHandle, numRows int64, by []string, isHead bool) (*TableHandle, error) {
	return ts.makeRequest(ctx, table, func(ctx ctxt, resultId ticketRef, sourceId tblRef) (tblResp, error) {
		req := tablepb2.HeadOrTailByRequest{ResultId: resultId, SourceId: sourceId, NumRows: numRows, GroupByColumnSpecs: by}
		if isHead {
			return ts.stub.HeadBy(ctx, &req)
		} else {
			return ts.stub.TailBy(ctx, &req)
		}
	})
}

func (ts *tableStub) dedicatedAggOp(ctx context.Context, table *TableHandle, by []string, countColumn string, kind tablepb2.ComboAggregateRequest_AggType) (*TableHandle, error) {
	return ts.makeRequest(ctx, table, func(ctx ctxt, resultId ticketRef, sourceId tblRef) (tblResp, error) {
		var agg tablepb2.ComboAggregateRequest_Aggregate
		if kind == tablepb2.ComboAggregateRequest_COUNT && countColumn != "" {
			agg = tablepb2.ComboAggregateRequest_Aggregate{Type: kind, ColumnName: countColumn}
		} else {
			agg = tablepb2.ComboAggregateRequest_Aggregate{Type: kind}
		}

		aggs := []*tablepb2.ComboAggregateRequest_Aggregate{&agg}

		req := tablepb2.ComboAggregateRequest{ResultId: resultId, SourceId: sourceId, Aggregates: aggs, GroupByColumns: by}
		return ts.stub.ComboAggregate(ctx, &req)
	})
}

func (ts *tableStub) ungroup(ctx context.Context, table *TableHandle, cols []string, nullFill bool) (*TableHandle, error) {
	return ts.makeRequest(ctx, table, func(ctx ctxt, resultId ticketRef, sourceId tblRef) (tblResp, error) {
		req := tablepb2.UngroupRequest{ResultId: resultId, SourceId: sourceId, NullFill: nullFill, ColumnsToUngroup: cols}
		return ts.stub.Ungroup(ctx, &req)
	})
}

func (ts *tableStub) aggBy(ctx context.Context, table *TableHandle, aggs *AggBuilder, by []string) (*TableHandle, error) {
	return ts.makeRequest(ctx, table, func(ctx ctxt, resultId ticketRef, sourceId tblRef) (tblResp, error) {
		var reqAggs []*tablepb2.ComboAggregateRequest_Aggregate
		for _, agg := range aggs.aggs {
			reqAgg := tablepb2.ComboAggregateRequest_Aggregate{Type: agg.kind, ColumnName: agg.columnName, MatchPairs: agg.matchPairs, Percentile: agg.percentile, AvgMedian: agg.avgMedian}
			reqAggs = append(reqAggs, &reqAgg)
		}

		req := &tablepb2.ComboAggregateRequest{ResultId: resultId, SourceId: sourceId, Aggregates: reqAggs, GroupByColumns: by}
		return ts.stub.ComboAggregate(ctx, req)
	})
}

func (ts *tableStub) merge(ctx context.Context, sortBy string, others []*TableHandle) (*TableHandle, error) {
	if ts.client.Closed() {
		return nil, ErrClosedClient
	}

	ctx = ts.client.withToken(ctx)

	resultId := ts.client.newTicket()

	sourceIds := make([]tblRef, len(others))
	for i, handle := range others {
		sourceIds[i] = &tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: handle.ticket}}
	}

	req := tablepb2.MergeTablesRequest{ResultId: &resultId, SourceIds: sourceIds, KeyColumn: sortBy}
	resp, err := ts.stub.MergeTables(ctx, &req)
	if err != nil {
		return nil, err
	}

	return parseCreationResponse(ts.client, resp)
}
