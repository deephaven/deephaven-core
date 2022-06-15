package client

import (
	"bytes"
	"context"
	"fmt"

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

	makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation
}

// A pointer into a Query DAG.
// Table operations can be performed on it to build up a query, which can then be executed using client.ExecQuery().
type QueryNode struct {
	// -1 refers to the queryBuilder's base table
	index   int
	builder *queryBuilder
}

func (qb QueryNode) addOp(op tableOp) QueryNode {
	qb.builder.ops = append(qb.builder.ops, op)
	return qb.builder.curRootNode()
}

// This is (some subgraph of) the Query DAG.
type queryBuilder struct {
	uniqueId int32
	table    *TableHandle
	ops      []tableOp
}

func (qb *queryBuilder) curRootNode() QueryNode {
	return QueryNode{index: len(qb.ops) - 1, builder: qb}
}

// Every op can be uniquely identified by its queryBuilder ID and its index within that queryBuilder.
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

// Returns the indexes of the node found
func (b *batchBuilder) needsExport(opIdx int, builderId int32) []int {
	var indices []int
	for i, node := range b.nodes {
		if node.index == opIdx && node.builder.uniqueId == builderId {
			indices = append(indices, i)
		}
	}
	return indices
}

// Returns a handle to the node's output table
func (b *batchBuilder) addGrpcOps(node QueryNode) *tablepb2.TableReference {
	var source *tablepb2.TableReference

	if node.index == -1 {
		return &tablepb2.TableReference{Ref: &tablepb2.TableReference_Ticket{Ticket: node.builder.table.ticket}}
	}

	// If the op is already in the list, we don't need to do it again.
	nodeKey := opKey{index: node.index, builderId: node.builder.uniqueId}
	if prevIdx, skip := b.finishedOps[nodeKey]; skip {
		// So just use the output of the existing occurence.
		return &tablepb2.TableReference{Ref: &tablepb2.TableReference_BatchOffset{BatchOffset: prevIdx}}
	}

	op := node.builder.ops[node.index]

	var childQueries []*tablepb2.TableReference = nil
	for _, child := range op.childQueries() {
		childRef := b.addGrpcOps(child)
		childQueries = append(childQueries, childRef)
	}

	var resultId *ticketpb2.Ticket = nil
	// Duplicate nodes that still need their own tickets
	var extraNodes []int
	if nodes := b.needsExport(node.index, node.builder.uniqueId); len(nodes) > 0 {
		t := b.client.newTicket()
		resultId = &t
		b.nodeOrder[nodes[0]] = resultId
		extraNodes = nodes[1:]
	}

	grpcOp := op.makeBatchOp(resultId, childQueries)
	b.grpcOps = append(b.grpcOps, &grpcOp)

	b.finishedOps[nodeKey] = int32(len(b.grpcOps)) - 1

	for _, extraNode := range extraNodes {
		sourceId := &tablepb2.TableReference{Ref: &tablepb2.TableReference_BatchOffset{BatchOffset: int32(len(b.grpcOps) - 1)}}
		t := b.client.newTicket()
		resultId = &t
		b.nodeOrder[extraNode] = resultId
		req := tablepb2.FetchTableRequest{ResultId: resultId, SourceId: sourceId}
		grpcOp := tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_FetchTable{FetchTable: &req}}
		b.grpcOps = append(b.grpcOps, &grpcOp)
	}

	source = &tablepb2.TableReference{Ref: &tablepb2.TableReference_BatchOffset{BatchOffset: int32(len(b.grpcOps) - 1)}}
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

	var output []*TableHandle

	for i, ticket := range nodeOrder {
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

func newQueryBuilder(client *Client, table *TableHandle) queryBuilder {
	return queryBuilder{uniqueId: client.newTicketNum(), table: table}
}

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

type timeTableOp struct {
	period    int64
	startTime int64
}

func (op timeTableOp) childQueries() []QueryNode {
	return nil
}

func (op timeTableOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 0, "wrong number of children for TimeTable")
	req := &tablepb2.TimeTableRequest{ResultId: resultId, PeriodNanos: op.period, StartTimeNanos: op.startTime}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_TimeTable{TimeTable: req}}
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

// Removes the columns with the specified names from the table.
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

// Adds additional columns to the table, calculated based on the given formulas.
// The new column is computed immediately and then stored in memory.
// See https://deephaven.io/core/docs/reference/table-operations/select/update/ for comparison with other column-changing methods.
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

// Adds additional columns to the table, calculated based on the given formulas.
// The data in the the new columns is computed on-demand and then cached.
// See https://deephaven.io/core/docs/reference/table-operations/select/lazy-update/ for comparison with other column-changing methods.
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
	assert(len(children) == 1, "wrong number of children for ViewOp")
	req := &tablepb2.SelectOrUpdateRequest{ResultId: resultId, SourceId: children[0], ColumnSpecs: op.formulas}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_View{View: req}}
}

// Create a new table displaying only the selected columns, optionally modified by formulas
// The data in the new columns is not stored in memory, and is recalculated from the formulas every time it is accessed.
// See https://deephaven.io/core/docs/reference/table-operations/select/view/ for comparison with other column-changing methods.
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

// Adds additional columns to the table, calculated based on the given formulas.
// The data in the new columns is not stored in memory, and is recalculated from the formulas every time it is accessed.
// See https://deephaven.io/core/docs/reference/table-operations/select/update-view/ for comparison with other column-changing methods.
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

// Adds additional columns to the table, calculated based on the given formulas.
// The data in the the new columns is computed immediately and stored in memory.
// See https://deephaven.io/core/docs/reference/table-operations/select/select/ for comparison with other column-changing methods.
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

// Returns a new table containing only unique rows that have the specified columns
func (qb QueryNode) SelectDistinct(columnNames ...string) QueryNode {
	return qb.addOp(selectDistinctOp{child: qb, cols: columnNames})
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

// Returns a table containing only rows that match the given filters
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

// Selects only the first `numRows` rows of the table
func (qb QueryNode) Head(numRows int64) QueryNode {
	return qb.addOp(headOrTailOp{child: qb, numRows: numRows, isTail: false})
}

// Selects only the last `numRows` rows of the table
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

// Performs a group aggregation on the given columns and then a Head request.
func (qb QueryNode) HeadBy(numRows int64, by ...string) QueryNode {
	return qb.addOp(headOrTailByOp{child: qb, numRows: numRows, by: by, isTail: false})
}

// Performs a group aggregation on the given columns and then a Tail request
func (qb QueryNode) TailBy(numRows int64, by ...string) QueryNode {
	return qb.addOp(headOrTailByOp{child: qb, numRows: numRows, by: by, isTail: true})
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

// Ungroups the table. The ungroup columns must be arrays.
// `nullFill` indicates whether or not missing cells may be filled with null, default is true
func (qb QueryNode) Ungroup(cols []string, nullFill bool) QueryNode {
	return qb.addOp(ungroupOp{child: qb, colNames: cols, nullFill: nullFill})
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

// Performs a group-by aggregation on the table.
// Columns not in the aggregation become array-type.
// If no group-by columns are given, the content of each column is grouped into its own array.
func (qb QueryNode) GroupBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_GROUP})
}

// Performs a first-by aggregation on the table, i.e. it returns a table that contains the first row of each distinct group.
func (qb QueryNode) FirstBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_FIRST})
}

// Performs a last-by aggregation on the table, i.e. it returns a table that contains the last rrow of each distinct group.
func (qb QueryNode) LastBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_LAST})
}

// Performs a sum-by aggregation on the table. Columns not used in the grouping must be numeric.
func (qb QueryNode) SumBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_SUM})
}

// Performs a sum-absolute-value-by agggregation on the table. Columns not used in the grouping must be numeric.
func (qb QueryNode) AbsSumBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_ABS_SUM})
}

// Performs an average-by aggregation on the table. Columns not used in the grouping must be numeric.
func (qb QueryNode) AvgBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_AVG})
}

// Performs a standard-deviation-by aggregation on the table. Columns not used in the grouping must be numeric.
func (qb QueryNode) StdBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_STD})
}

// Performs a variance-by aggregation on the table. Columns not used in the grouping must be numeric.
func (qb QueryNode) VarBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_VAR})
}

// Performs a median-by aggregation on the table. Columns not used in the grouping must be numeric.
func (qb QueryNode) MedianBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_MEDIAN})
}

// Performs a minimum-by aggregation on the table. Columns not used in the grouping must be numeric.
func (qb QueryNode) MinBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_MIN})
}

// Performs a maximum-by aggregation on the table. Columns not used in the grouping must be numeric.
func (qb QueryNode) MaxBy(by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, kind: tablepb2.ComboAggregateRequest_MAX})
}

// Performs a count-by aggregation on the table.
// The count of each group is stored in a new column named after the `resultCol` argument.
func (qb QueryNode) CountBy(resultCol string, by ...string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, colNames: by, countColumn: resultCol, kind: tablepb2.ComboAggregateRequest_COUNT})
}

// Counts the number of values in the specified column and returns it as a table with one row and one column.
func (qb QueryNode) Count(col string) QueryNode {
	return qb.addOp(dedicatedAggOp{child: qb, countColumn: col, kind: tablepb2.ComboAggregateRequest_COUNT})
}

type aggPart struct {
	matchPairs []string
	columnName string
	percentile float64
	avgMedian  bool
	kind       tablepb2.ComboAggregateRequest_AggType
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

// Performs a count aggregation on the table.
// The count of each group is stored in a new column named after the `col` argument.
func (b *AggBuilder) Count(col string) *AggBuilder {
	b.addAgg(aggPart{columnName: col, kind: tablepb2.ComboAggregateRequest_COUNT})
	return b
}

// Performs a sum-by aggregation on the table.
func (b *AggBuilder) Sum(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_SUM})
	return b
}

// Performs a sum-absolute-value-by agggregation on the table.
func (b *AggBuilder) AbsSum(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_ABS_SUM})
	return b
}

// Performs a group-by aggregation on the table.
// Columns not in the aggregation become array-type.
// If no group-by columns are given, the content of each column is grouped into its own array.
func (b *AggBuilder) Group(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_GROUP})
	return b
}

// Performs an average-by aggregation on the table.
func (b *AggBuilder) Avg(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_AVG})
	return b
}

// Performs a first-by aggregation on the table, i.e. it returns a table that contains the first row of each distinct group.
func (b *AggBuilder) First(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_FIRST})
	return b
}

// Performs a last-by aggregation on the table, i.e. it returns a table that contains the last row of each distinct group.
func (b *AggBuilder) Last(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_LAST})
	return b
}

// Performs a minimum-by aggregation on the table.
func (b *AggBuilder) Min(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_MIN})
	return b
}

// Performs a maximum-by aggregation on the table.
func (b *AggBuilder) Max(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_MAX})
	return b
}

// Performs a median-by aggregation on the table.
func (b *AggBuilder) Median(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_MEDIAN})
	return b
}

// Performs a percentile aggregation on the table.
func (b *AggBuilder) Percentile(percentile float64, cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, percentile: percentile, kind: tablepb2.ComboAggregateRequest_PERCENTILE})
	return b
}

// Performs a standard-deviation-by aggregation on the table.
func (b *AggBuilder) StdDev(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_STD})
	return b
}

// Performs a variance-by aggregation on the table.
func (b *AggBuilder) Variance(cols ...string) *AggBuilder {
	b.addAgg(aggPart{matchPairs: cols, kind: tablepb2.ComboAggregateRequest_VAR})
	return b
}

// Performs a weighted average on the table. `weightCol` is used as the weights.
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

// Performs an aggregation, grouping by the given columns.
// See the docs on AggBuilder for details on what each of the aggregation types do.
func (qb QueryNode) AggBy(agg *AggBuilder, by ...string) QueryNode {
	aggs := make([]aggPart, len(agg.aggs))
	copy(aggs, agg.aggs)
	return qb.addOp(aggByOp{child: qb, colNames: by, aggs: aggs})
}

const (
	joinOpCross   = iota
	joinOpNatural = iota
	joinOpExact   = iota
)

type joinOp struct {
	leftTable      QueryNode
	rightTable     QueryNode
	columnsToMatch []string
	columnsToAdd   []string
	reserveBits    int32
	kind           int
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

// Performs a cross-join with this table as the left table.
//
// `on` is the columns to match, which can be a column name or an equals expression (e.g. "colA = colB").
//
// `joins` is the columns to add from the right table.
//
// `reserveBits` is the number of bits of key-space to initially reserve per group, default is 10.
func (qb QueryNode) Join(rightTable QueryNode, on []string, joins []string, reserveBits int32) QueryNode {
	op := joinOp{
		leftTable:      qb,
		rightTable:     rightTable,
		columnsToMatch: on,
		columnsToAdd:   joins,
		reserveBits:    reserveBits,
		kind:           joinOpCross,
	}
	return qb.addOp(op)
}

// Performs an exact-join with this table as the left table.
//
// `on` is the columns to match, which can be a column name or an equals expression (e.g. "colA = colB").
//
// `joins` is the columns to add from the right table.
func (qb QueryNode) ExactJoin(rightTable QueryNode, on []string, joins []string) QueryNode {
	op := joinOp{
		leftTable:      qb,
		rightTable:     rightTable,
		columnsToMatch: on,
		columnsToAdd:   joins,
		kind:           joinOpExact,
	}
	return qb.addOp(op)
}

// Performs a natural-join with this table as the left table.
//
// `on` is the columns to match, which can be a column name or an equals expression (e.g. "colA = colB").
//
// `joins` is the columns to add from the right table.
func (qb QueryNode) NaturalJoin(rightTable QueryNode, on []string, joins []string) QueryNode {
	op := joinOp{
		leftTable:      qb,
		rightTable:     rightTable,
		columnsToMatch: on,
		columnsToAdd:   joins,
		kind:           joinOpNatural,
	}
	return qb.addOp(op)
}

// A comparison rule for use with [QueryNode.AsOfJoin].
// See its documentation for more details.
const (
	MatchRuleLessThanEqual    = iota
	MatchRuleLessThan         = iota
	MatchRuleGreaterThanEqual = iota
	MatchRuleGreaterThan      = iota
)

type asOfJoinOp struct {
	leftTable      QueryNode
	rightTable     QueryNode
	columnsToMatch []string
	columnsToAdd   []string
	matchRule      int
}

func (op asOfJoinOp) childQueries() []QueryNode {
	return []QueryNode{op.leftTable, op.rightTable}
}

func (op asOfJoinOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == 2, "wrong number of children for AsOfJoin")

	leftId := children[0]
	rightId := children[1]

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

	req := &tablepb2.AsOfJoinTablesRequest{ResultId: resultId, LeftId: leftId, RightId: rightId, ColumnsToMatch: op.columnsToMatch, ColumnsToAdd: op.columnsToAdd, AsOfMatchRule: matchRule}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_AsOfJoin{AsOfJoin: req}}
}

// Performs an as-of-join with this table as the left table.
//
// `on` is the columns to match, which can be a column name or an equals expression (e.g. "colA = colB").
//
// `joins` is the columns to add from the right table.
//
// `matchRule` is the match rule for the join, default is MatchRuleLessThanEqual normally, or MatchRuleGreaterThanEqual or a reverse-as-of-join
func (qb QueryNode) AsOfJoin(rightTable QueryNode, on []string, joins []string, matchRule int) QueryNode {
	op := asOfJoinOp{
		leftTable:      qb,
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

func (op mergeOp) makeBatchOp(resultId *ticketpb2.Ticket, children []*tablepb2.TableReference) tablepb2.BatchTableRequest_Operation {
	assert(len(children) == len(op.children), "wrong number of children for Merge")

	req := &tablepb2.MergeTablesRequest{ResultId: resultId, SourceIds: children, KeyColumn: op.sortBy}
	return tablepb2.BatchTableRequest_Operation{Op: &tablepb2.BatchTableRequest_Operation_Merge{Merge: req}}
}

// Combines additional tables into one table by putting their rows on top of each other.
// All tables involved must have the same columns.
// If sortBy is provided, the resulting table will be sorted based on that column.
func (qb QueryNode) Merge(sortBy string, others ...QueryNode) QueryNode {
	children := make([]QueryNode, len(others)+1)
	children[0] = qb
	copy(children[1:], others)

	qb.builder.ops = append(qb.builder.ops, mergeOp{children: children, sortBy: sortBy})
	return qb.builder.curRootNode()
}
