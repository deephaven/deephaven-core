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

func (qb QueryNode) addOp(op tableOp) QueryNode {
	qb.builder.ops = append(qb.builder.ops, op)
	return qb.builder.curRootNode()
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

func execQuery(client *Client, ctx context.Context, nodes []QueryNode) ([]*TableHandle, error) {
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

	var output []*TableHandle

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

type emptyTableOp struct {
	numRows int64
}

func (op emptyTableOp) childQueries() []QueryNode {
	return nil
}

func (op emptyTableOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for EmptyTable")
	assert(sourceId == nil, "non-nil sourceId for EmptyTable")
	req := &tablepb2.EmptyTableRequest{ResultId: resultId, Size: op.numRows}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_EmptyTable{EmptyTable: req}}
}

type timeTableOp struct {
	period    int64
	startTime int64
}

func (op timeTableOp) childQueries() []QueryNode {
	return nil
}

func (op timeTableOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for TimeTable")
	assert(sourceId == nil, "non-nil sourceId for TimeTable")
	req := &tablepb2.TimeTableRequest{ResultId: resultId, PeriodNanos: op.period, StartTimeNanos: op.startTime}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_TimeTable{TimeTable: req}}
}

type dropColumnsOp struct {
	cols []string
}

func (op dropColumnsOp) childQueries() []QueryNode {
	return nil
}

func (op dropColumnsOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for DropColumns")
	assert(sourceId != nil, "nil sourceId for DropColumns")
	req := &tablepb2.DropColumnsRequest{ResultId: resultId, SourceId: sourceId, ColumnNames: op.cols}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_DropColumns{DropColumns: req}}
}

// Removes the columns with the specified names from the table.
func (qb QueryNode) DropColumns(cols ...string) QueryNode {
	return qb.addOp(dropColumnsOp{cols: cols})
}

type updateOp struct {
	formulas []string
}

func (op updateOp) childQueries() []QueryNode {
	return nil
}

func (op updateOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for Update")
	assert(sourceId != nil, "nil sourceId for Update")
	req := &tablepb2.SelectOrUpdateRequest{ResultId: resultId, SourceId: sourceId, ColumnSpecs: op.formulas}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_Update{Update: req}}
}

// Adds additional columns to the table, calculated based on the given formulas
func (qb QueryNode) Update(formulas ...string) QueryNode {
	return qb.addOp(updateOp{formulas: formulas})
}

type lazyUpdateOp struct {
	formulas []string
}

func (op lazyUpdateOp) childQueries() []QueryNode {
	return nil
}

func (op lazyUpdateOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for LazyUpdate")
	assert(sourceId != nil, "nil sourceId for LazyUpdate")
	req := &tablepb2.SelectOrUpdateRequest{ResultId: resultId, SourceId: sourceId, ColumnSpecs: op.formulas}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_LazyUpdate{LazyUpdate: req}}
}

// Performs a lazy-update
func (qb QueryNode) LazyUpdate(formulas ...string) QueryNode {
	return qb.addOp(lazyUpdateOp{formulas: formulas})
}

type viewOp struct {
	formulas []string
}

func (op viewOp) childQueries() []QueryNode {
	return nil
}

func (op viewOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for ViewOp")
	assert(sourceId != nil, "nil sourceId for ViewOp")
	req := &tablepb2.SelectOrUpdateRequest{ResultId: resultId, SourceId: sourceId, ColumnSpecs: op.formulas}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_View{View: req}}
}

// Create a new table displaying only the selected columns, optionally modified by formulas
func (qb QueryNode) View(formulas ...string) QueryNode {
	return qb.addOp(viewOp{formulas: formulas})
}

type updateViewOp struct {
	formulas []string
}

func (op updateViewOp) childQueries() []QueryNode {
	return nil
}

func (op updateViewOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for UpdateView")
	assert(sourceId != nil, "nil sourceId for UpdateView")
	req := &tablepb2.SelectOrUpdateRequest{ResultId: resultId, SourceId: sourceId, ColumnSpecs: op.formulas}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_UpdateView{UpdateView: req}}
}

func (qb QueryNode) UpdateView(formulas ...string) QueryNode {
	return qb.addOp(updateViewOp{formulas: formulas})
}

type selectOp struct {
	formulas []string
}

func (op selectOp) childQueries() []QueryNode {
	return nil
}

func (op selectOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for Select")
	assert(sourceId != nil, "nil sourceId for Select")
	req := &tablepb2.SelectOrUpdateRequest{ResultId: resultId, SourceId: sourceId, ColumnSpecs: op.formulas}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_Select{Select: req}}
}

func (qb QueryNode) Select(formulas ...string) QueryNode {
	return qb.addOp(selectOp{formulas: formulas})
}

type selectDistinctOp struct {
	cols []string
}

func (op selectDistinctOp) childQueries() []QueryNode {
	return nil
}

func (op selectDistinctOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for SelectDistinct")
	assert(sourceId != nil, "nil sourceId for SelectDistinct")
	req := &tablepb2.SelectDistinctRequest{ResultId: resultId, SourceId: sourceId, ColumnNames: op.cols}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_SelectDistinct{SelectDistinct: req}}
}

// Returns a new table containing only unique rows that have the specified columns
func (qb QueryNode) SelectDistinct(columnNames ...string) QueryNode {
	return qb.addOp(selectDistinctOp{cols: columnNames})
}

type SortColumn struct {
	colName    string
	descending bool
}

// Specifies that a particular column should be sorted in ascending order
func SortAsc(colName string) SortColumn {
	return SortColumn{colName: colName, descending: false}
}

// Specifies that a particular column should be sorted in descending order
func SortDsc(colName string) SortColumn {
	return SortColumn{colName: colName, descending: true}
}

type sortOp struct {
	columns []SortColumn
}

func (op sortOp) childQueries() []QueryNode {
	return nil
}

func (op sortOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for Sort")
	assert(sourceId != nil, "nil sourceId for Sort")

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

	req := &tablepb2.SortTableRequest{ResultId: resultId, SourceId: sourceId, Sorts: sorts}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_Sort{Sort: req}}
}

// Returns a new table sorted in ascending order based on the given columns.
func (qb QueryNode) Sort(cols ...string) QueryNode {
	var columns []SortColumn
	for _, col := range cols {
		columns = append(columns, SortAsc(col))
	}
	return qb.SortBy(columns...)
}

// Returns a new table sorted in the order specified by each column
func (qb QueryNode) SortBy(cols ...SortColumn) QueryNode {
	return qb.addOp(sortOp{columns: cols})
}

type filterOp struct {
	filters []string
}

func (op filterOp) childQueries() []QueryNode {
	return nil
}

func (op filterOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for Filter")
	assert(sourceId != nil, "nil sourceId for Filter")
	req := &tablepb2.UnstructuredFilterTableRequest{ResultId: resultId, SourceId: sourceId, Filters: op.filters}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_UnstructuredFilter{UnstructuredFilter: req}}
}

// Returns a table containing only rows that match the given filters
func (qb QueryNode) Where(filters []string) QueryNode {
	return qb.addOp(filterOp{filters: filters})
}

type headOrTailOp struct {
	numRows int64
	isTail  bool
}

func (op headOrTailOp) childQueries() []QueryNode {
	return nil
}

func (op headOrTailOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for Head or Tail")
	assert(sourceId != nil, "nil sourceId for Head or Tail")
	req := &tablepb2.HeadOrTailRequest{ResultId: resultId, SourceId: sourceId, NumRows: op.numRows}

	if op.isTail {
		return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_Tail{Tail: req}}
	} else {
		return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_Head{Head: req}}
	}
}

// Selects only the first `numRows` rows of the table
func (qb QueryNode) Head(numRows int64) QueryNode {
	return qb.addOp(headOrTailOp{numRows: numRows, isTail: false})
}

// Selects only the last `numRows` rows of the table
func (qb QueryNode) Tail(numRows int64) QueryNode {
	return qb.addOp(headOrTailOp{numRows: numRows, isTail: true})
}

type headOrTailByOp struct {
	numRows int64
	by      []string
	isTail  bool
}

func (op headOrTailByOp) childQueries() []QueryNode {
	return nil
}

func (op headOrTailByOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for HeadBy or TailBy")
	assert(sourceId != nil, "nil sourceId for HeadBy or TailBy")

	req := &tablepb2.HeadOrTailByRequest{ResultId: resultId, SourceId: sourceId, NumRows: op.numRows, GroupByColumnSpecs: op.by}

	if op.isTail {
		return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_TailBy{TailBy: req}}
	} else {
		return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_HeadBy{HeadBy: req}}
	}
}

// Performs a group aggregation on the given columns and then a Head request
func (qb QueryNode) HeadBy(numRows int64, by []string) QueryNode {
	return qb.addOp(headOrTailByOp{numRows: numRows, by: by, isTail: false})
}

// Performs a group aggregation on the given columns and then a Tail request
func (qb QueryNode) TailBy(numRows int64, by []string) QueryNode {
	return qb.addOp(headOrTailByOp{numRows: numRows, by: by, isTail: true})
}

type ungroupOp struct {
	colNames []string
	nullFill bool
}

func (op ungroupOp) childQueries() []QueryNode {
	return nil
}

func (op ungroupOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for Ungroup")
	assert(sourceId != nil, "nil sourceId for Ungroup")
	req := &tablepb2.UngroupRequest{ResultId: resultId, SourceId: sourceId, ColumnsToUngroup: op.colNames, NullFill: op.nullFill}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_Ungroup{Ungroup: req}}
}

// Ungroups the table. The ungroup columns must be arrays.
// `nullFill` indicates whether or not missing cells may be filled with null, default is true
func (qb QueryNode) Ungroup(cols []string, nullFill bool) QueryNode {
	return qb.addOp(ungroupOp{colNames: cols, nullFill: nullFill})
}

type dedicatedAggOp struct {
	colNames    []string
	countColumn string
	kind        tablepb2.ComboAggregateRequest_AggType
}

func (op dedicatedAggOp) childQueries() []QueryNode {
	return nil
}

func (op dedicatedAggOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for dedicated aggregation")
	assert(sourceId != nil, "nil sourceId for dedicated aggregation")

	var agg tablepb2.ComboAggregateRequest_Aggregate
	if op.kind == tablepb2.ComboAggregateRequest_COUNT && op.countColumn != "" {
		agg = tablepb2.ComboAggregateRequest_Aggregate{Type: op.kind, ColumnName: op.countColumn}
	} else {
		agg = tablepb2.ComboAggregateRequest_Aggregate{Type: op.kind}
	}

	aggs := []*tablepb2.ComboAggregateRequest_Aggregate{&agg}

	req := &tablepb2.ComboAggregateRequest{ResultId: resultId, SourceId: sourceId, Aggregates: aggs, GroupByColumns: op.colNames}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_ComboAggregate{ComboAggregate: req}}
}

// Performs a group-by aggregation on the table.
// Columns not in the aggregation become array-type.
// If no group-by columns are given, the content of each column is grouped into its own array.
func (qb QueryNode) GroupBy(by []string) QueryNode {
	return qb.addOp(dedicatedAggOp{colNames: by, kind: tablepb2.ComboAggregateRequest_GROUP})
}

// Performs a first-by aggregation on the table, i.e. it returns a table that contains the first row of each distinct group.
func (qb QueryNode) FirstBy(by []string) QueryNode {
	return qb.addOp(dedicatedAggOp{colNames: by, kind: tablepb2.ComboAggregateRequest_FIRST})
}

// Performs a last-by aggregation on the table, i.e. it returns a table that contains the last rrow of each distinct group.
func (qb QueryNode) LastBy(by []string) QueryNode {
	return qb.addOp(dedicatedAggOp{colNames: by, kind: tablepb2.ComboAggregateRequest_LAST})
}

// Performs a sum-by aggregation on the table. Columns not used in the grouping must be numeric.
func (qb QueryNode) SumBy(by []string) QueryNode {
	return qb.addOp(dedicatedAggOp{colNames: by, kind: tablepb2.ComboAggregateRequest_SUM})
}

// Performs an average-by aggregation on the table. Columns not used in the grouping must be numeric.
func (qb QueryNode) AvgBy(by []string) QueryNode {
	return qb.addOp(dedicatedAggOp{colNames: by, kind: tablepb2.ComboAggregateRequest_AVG})
}

// Performs a standard-deviation-by aggregation on the table. Columns not used in the grouping must be numeric.
func (qb QueryNode) StdBy(by []string) QueryNode {
	return qb.addOp(dedicatedAggOp{colNames: by, kind: tablepb2.ComboAggregateRequest_STD})
}

// Performs a variance-by aggregation on the table. Columns not used in the grouping must be numeric.
func (qb QueryNode) VarBy(by []string) QueryNode {
	return qb.addOp(dedicatedAggOp{colNames: by, kind: tablepb2.ComboAggregateRequest_VAR})
}

// Performs a median-by aggregation on the table. Columns not used in the grouping must be numeric.
func (qb QueryNode) MedianBy(by []string) QueryNode {
	return qb.addOp(dedicatedAggOp{colNames: by, kind: tablepb2.ComboAggregateRequest_MEDIAN})
}

// Performs a minimum-by aggregation on the table. Columns not used in the grouping must be numeric.
func (qb QueryNode) MinBy(by []string) QueryNode {
	return qb.addOp(dedicatedAggOp{colNames: by, kind: tablepb2.ComboAggregateRequest_MIN})
}

// Performs a maximum-by aggregation on the table. Columns not used in the grouping must be numeric.
func (qb QueryNode) MaxBy(by []string) QueryNode {
	return qb.addOp(dedicatedAggOp{colNames: by, kind: tablepb2.ComboAggregateRequest_MAX})
}

// Performs a count-by aggregation on the table.
// The count of each group is stored in a new column named after the `resultCol` argument.
func (qb QueryNode) CountBy(resultCol string, by []string) QueryNode {
	return qb.addOp(dedicatedAggOp{colNames: by, countColumn: resultCol, kind: tablepb2.ComboAggregateRequest_COUNT})
}

// Counts the number of values in the specified column and returns it as a table with one row and one column.
func (qb QueryNode) Count(col string) QueryNode {
	return qb.addOp(dedicatedAggOp{countColumn: col, kind: tablepb2.ComboAggregateRequest_COUNT})
}

type aggPart struct {
	matchPairs []string
	columnName string
	percentile float64
	avgMedian  bool
	kind       tablepb2.ComboAggregateRequest_AggType
}

// AggBuilder is the main way to construct aggregations with multiple parts in them.
type AggBuilder struct {
	aggs []aggPart
}

func NewAggBuilder() *AggBuilder {
	return &AggBuilder{}
}

func (b *AggBuilder) addAgg(part aggPart) {
	b.aggs = append(b.aggs, part)
}

func (b *AggBuilder) Count(col string) *AggBuilder {
	b.addAgg(aggPart{columnName: col, kind: tablepb2.ComboAggregateRequest_COUNT})
	return b
}

func (b *AggBuilder) Sum(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_SUM})
	return b
}

func (b *AggBuilder) AbsSum(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_ABS_SUM})
	return b
}

func (b *AggBuilder) Group(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_GROUP})
	return b
}

func (b *AggBuilder) Avg(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_AVG})
	return b
}

func (b *AggBuilder) First(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_FIRST})
	return b
}

func (b *AggBuilder) Last(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_LAST})
	return b
}

func (b *AggBuilder) Min(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_MIN})
	return b
}

func (b *AggBuilder) Max(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_MAX})
	return b
}

func (b *AggBuilder) Median(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_MEDIAN})
	return b
}

func (b *AggBuilder) Percentile(percentile float64, cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, percentile: percentile, kind: tablepb2.ComboAggregateRequest_PERCENTILE})
	return b
}

func (b *AggBuilder) StdDev(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_STD})
	return b
}

func (b *AggBuilder) Variance(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_VAR})
	return b
}

func (b *AggBuilder) WeightedAvg(weightCol string, cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, columnName: weightCol, kind: tablepb2.ComboAggregateRequest_WEIGHTED_AVG})
	return b
}

type aggByOp struct {
	colNames []string
	aggs     []aggPart
}

func (op aggByOp) childQueries() []QueryNode {
	return nil
}

func (op aggByOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for AggBy")
	assert(sourceId != nil, "nil sourceId for dedicated AggBy")

	var aggs []*tablepb2.ComboAggregateRequest_Aggregate
	for _, agg := range op.aggs {
		reqAgg := tablepb2.ComboAggregateRequest_Aggregate{Type: agg.kind, ColumnName: agg.columnName, MatchPairs: agg.matchPairs, Percentile: agg.percentile, AvgMedian: agg.avgMedian}
		aggs = append(aggs, &reqAgg)
	}

	req := &tablepb2.ComboAggregateRequest{ResultId: resultId, SourceId: sourceId, Aggregates: aggs, GroupByColumns: op.colNames}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_ComboAggregate{ComboAggregate: req}}
}

func (qb QueryNode) AggBy(agg *AggBuilder, by ...string) QueryNode {
	aggs := make([]aggPart, len(agg.aggs))
	copy(aggs, agg.aggs)
	return qb.addOp(aggByOp{colNames: by, aggs: aggs})
}

const (
	joinOpCross   = iota
	joinOpNatural = iota
	joinOpExact   = iota
)

type joinOp struct {
	rightTable     QueryNode
	columnsToMatch []string
	columnsToAdd   []string
	reserveBits    int32
	kind           int
}

func (op joinOp) childQueries() []QueryNode {
	return []QueryNode{op.rightTable}
}

func (op joinOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 1, "wrong number of children for CrossJoin, NaturalJoin, or ExactJoin")
	assert(sourceId != nil, "nil sourceId for CrossJoin, NaturalJoin, or ExactJoin")

	rightId := children[0]

	switch op.kind {
	case joinOpCross:
		req := &tablepb2.CrossJoinTablesRequest{ResultId: resultId, LeftId: sourceId, RightId: rightId, ColumnsToMatch: op.columnsToMatch, ColumnsToAdd: op.columnsToAdd, ReserveBits: op.reserveBits}
		return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_CrossJoin{CrossJoin: req}}
	case joinOpNatural:
		req := &tablepb2.NaturalJoinTablesRequest{ResultId: resultId, LeftId: sourceId, RightId: rightId, ColumnsToMatch: op.columnsToMatch, ColumnsToAdd: op.columnsToAdd}
		return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_NaturalJoin{NaturalJoin: req}}
	case joinOpExact:
		req := &tablepb2.ExactJoinTablesRequest{ResultId: resultId, LeftId: sourceId, RightId: rightId, ColumnsToMatch: op.columnsToMatch, ColumnsToAdd: op.columnsToAdd}
		return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_ExactJoin{ExactJoin: req}}
	default:
		panic("invalid join kind")
	}
}

// Performs a cross-join with this table as the left table.
// `on` is the columns to match, which can be a column name or an equals expression (e.g. "colA = colB").
// `joins` is the columns to add from the right table.
// `reserveBits` is the number of bits of key-space to initially reserve per group, default is 10.
func (qb QueryNode) CrossJoin(rightTable QueryNode, on []string, joins []string, reserveBits int32) QueryNode {
	op := joinOp{
		rightTable:     rightTable,
		columnsToMatch: on,
		columnsToAdd:   joins,
		reserveBits:    reserveBits,
		kind:           joinOpCross,
	}
	return qb.addOp(op)
}

// Performs an exact-join with this table as the left table.
// `on` is the columns to match, which can be a column name or an equals expression (e.g. "colA = colB").
// `joins` is the columns to add from the right table.
func (qb QueryNode) ExactJoin(rightTable QueryNode, on []string, joins []string) QueryNode {
	op := joinOp{
		rightTable:     rightTable,
		columnsToMatch: on,
		columnsToAdd:   joins,
		kind:           joinOpExact,
	}
	return qb.addOp(op)
}

// Performs a natural-join with this table as the left table.
// `on` is the columns to match, which can be a column name or an equals expression (e.g. "colA = colB").
// `joins` is the columns to add from the right table.
func (qb QueryNode) NaturalJoin(rightTable QueryNode, on []string, joins []string) QueryNode {
	op := joinOp{
		rightTable:     rightTable,
		columnsToMatch: on,
		columnsToAdd:   joins,
		kind:           joinOpNatural,
	}
	return qb.addOp(op)
}

const (
	MatchRuleLessThanEqual    = iota
	MatchRuleLessThan         = iota
	MatchRuleGreaterThanEqual = iota
	MatchRuleGreaterThan      = iota
)

type asOfJoinOp struct {
	rightTable     QueryNode
	columnsToMatch []string
	columnsToAdd   []string
	matchRule      int
}

func (op asOfJoinOp) childQueries() []QueryNode {
	return []QueryNode{op.rightTable}
}

func (op asOfJoinOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 1, "wrong number of children for AsOfJoin")
	assert(sourceId != nil, "nil sourceId for AsOfJoin")

	rightId := children[0]

	var matchRule tablepb2.AsOfJoinTablesRequest_MatchRule
	switch op.matchRule {
	case MatchRuleLessThanEqual:
		matchRule = tablepb2.AsOfJoinTablesRequest_LESS_THAN_EQUAL
	case MatchRuleLessThan:
		matchRule = tablepb2.AsOfJoinTablesRequest_LESS_THAN
	case MatchRuleGreaterThanEqual:
		matchRule = tablepb2.AsOfJoinTablesRequest_GREATER_THAN_EQUAL
	case MatchRuleGreaterThan:
		matchRule = tablepb2.AsOfJoinTablesRequest_GREATER_THAN
	default:
		panic("invalid match rule")
	}

	req := &tablepb2.AsOfJoinTablesRequest{ResultId: resultId, LeftId: sourceId, RightId: rightId, ColumnsToMatch: op.columnsToMatch, ColumnsToAdd: op.columnsToAdd, AsOfMatchRule: matchRule}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_AsOfJoin{AsOfJoin: req}}
}

// Performs an as-of-join with this table as the left table.
// `on` is the columns to match, which can be a column name or an equals expression (e.g. "colA = colB").
// `joins` is the columns to add from the right table.
// `matchRule` is the match rule for the join, default is MatchRuleLessThanEqual normally, or MatchRuleGreaterThanEqual or a reverse-as-of-join
func (qb QueryNode) AsOfJoin(rightTable QueryNode, on []string, joins []string, matchRule int) QueryNode {
	op := asOfJoinOp{
		rightTable:     rightTable,
		columnsToMatch: on,
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

func (op mergeOp) makeBatchOp(resultId *ticketpb2.Ticket, sourceId *tablepb2.TableReference, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
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
	qb.builder.ops = append(qb.builder.ops, mergeOp{children: others, sortBy: sortBy})
	return qb.builder.curRootNode()
}
