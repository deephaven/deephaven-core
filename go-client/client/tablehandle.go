package client

import (
	"context"
	"log"

	"github.com/apache/arrow/go/v8/arrow"

	tablepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/table"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"
)

// A reference to a table stored on the deephaven server.
//
// It should be eventually released using Release once it is no longer needed.
type TableHandle struct {
	client   *Client
	ticket   *ticketpb2.Ticket
	schema   *arrow.Schema
	size     int64
	isStatic bool
}

func newTableHandle(client *Client, ticket *ticketpb2.Ticket, schema *arrow.Schema, size int64, isStatic bool) *TableHandle {
	return &TableHandle{
		client:   client,
		ticket:   ticket,
		schema:   schema,
		size:     size,
		isStatic: isStatic,
	}
}

// Returns true if this table does not change over time.
// This will be false for things like streaming tables or timetables.
func (th *TableHandle) IsStatic() bool {
	return th.isStatic
}

// Downloads the current state of the table on the server and returns it as a Record.
//
// If a Record is returned successfully, it must be freed later with `record.Release()`
func (th *TableHandle) Snapshot(ctx context.Context) (arrow.Record, error) {
	return th.client.snapshotRecord(ctx, th.ticket)
}

// Creates a new query based on this table. Table operations can be performed on query nodes,
//
func (th *TableHandle) Query() QueryNode {
	qb := newQueryBuilder(th.client, th)
	return qb.curRootNode()
}

// Releases this table handle's resources on the server. The TableHandle is no longer usable after Release is called.
func (th *TableHandle) Release(ctx context.Context) error {
	if th.client != nil {
		err := th.client.release(ctx, th.ticket)
		if err != nil {
			log.Println("unable to release table:", err.Error())
			return err
		}

		th.client = nil
		th.ticket = nil
		th.schema = nil
	}
	return nil
}

// Returns a new table without the given columns.
func (th *TableHandle) DropColumns(ctx context.Context, cols ...string) (*TableHandle, error) {
	return th.client.dropColumns(ctx, th, cols)
}

// Returns a new table with additional columns calculated according to the formulas
// (See https://deephaven.io/core/docs/how-to-guides/use-select-view-update/).
func (th *TableHandle) Update(ctx context.Context, formulas ...string) (*TableHandle, error) {
	return th.client.update(ctx, th, formulas)
}

// Returns a new table with additional columns calculated according to the formulas
// (See https://deephaven.io/core/docs/how-to-guides/use-select-view-update/).
func (th *TableHandle) LazyUpdate(ctx context.Context, formulas ...string) (*TableHandle, error) {
	return th.client.lazyUpdate(ctx, th, formulas)
}

// Returns a new table with additional columns calculated according to the formulas
// (See https://deephaven.io/core/docs/how-to-guides/use-select-view-update/).
func (th *TableHandle) UpdateView(ctx context.Context, formulas ...string) (*TableHandle, error) {
	return th.client.updateView(ctx, th, formulas)
}

// Returns a new table with the columns specified by the formulas
// (See https://deephaven.io/core/docs/how-to-guides/use-select-view-update/).
func (th *TableHandle) View(ctx context.Context, formulas ...string) (*TableHandle, error) {
	return th.client.view(ctx, th, formulas)
}

// Returns a new table with the columns specified by the formulas.
//
// (See https://deephaven.io/core/docs/how-to-guides/use-select-view-update/)
func (th *TableHandle) Select(ctx context.Context, formulas ...string) (*TableHandle, error) {
	return th.client.selectTbl(ctx, th, formulas)
}

// Returns a new table containing only unique rows that have the specified columns.
// If no columns are provided, it will use every column in the table.
func (th *TableHandle) SelectDistinct(ctx context.Context, columns ...string) (*TableHandle, error) {
	return th.client.selectDistinct(ctx, th, columns)
}

// Returns a new table sorted in ascending order based on the given columns.
func (th *TableHandle) Sort(ctx context.Context, cols ...string) (*TableHandle, error) {
	var columns []SortColumn
	for _, col := range cols {
		columns = append(columns, SortAsc(col))
	}
	return th.SortBy(ctx, columns...)
}

// Returns a new table sorted in the order specified by each column
func (th *TableHandle) SortBy(ctx context.Context, cols ...SortColumn) (*TableHandle, error) {
	return th.client.sortBy(ctx, th, cols)
}

// Returns a new table containing only the rows that match the given filters.
func (th *TableHandle) Where(ctx context.Context, filters ...string) (*TableHandle, error) {
	return th.client.where(ctx, th, filters)
}

// Returns a new table containing only the first `numRows` rows
func (th *TableHandle) Head(ctx context.Context, numRows int64) (*TableHandle, error) {
	return th.client.headOrTail(ctx, th, numRows, true)
}

// Returns a new table containing only the last `numRows` rows
func (th *TableHandle) Tail(ctx context.Context, numRows int64) (*TableHandle, error) {
	return th.client.headOrTail(ctx, th, numRows, false)
}

// Performs a natural-join with this table as the left table.
//
// `on` is the columns to match, which can be a column name or an equals expression (e.g. "colA = colB").
//
// `joins` is the columns to add from the right table.
//
// See https://deephaven.io/core/docs/reference/table-operations/join/natural-join/ for details.
func (th *TableHandle) NaturalJoin(ctx context.Context, rightTable *TableHandle, on []string, joins []string) (*TableHandle, error) {
	return th.client.naturalJoin(ctx, th, rightTable, on, joins)
}

// Performs a cross-join with this table as the left table.
//
// `on` is the columns to match, which can be a column name or an equals expression (e.g. "colA = colB").
//
// `joins` is the columns to add from the right table.
//
// `reserveBits` is the number of bits of key-space to initially reserve per group, default is 10.
func (th *TableHandle) Join(ctx context.Context, rightTable *TableHandle, on []string, joins []string, reserveBits int32) (*TableHandle, error) {
	return th.client.crossJoin(ctx, th, rightTable, on, joins, reserveBits)
}

// Performs an exact-join with this table as the left table.
//
// `on` is the columns to match, which can be a column name or an equals expression (e.g. "colA = colB").
//
// `joins` is the columns to add from the right table.
func (th *TableHandle) ExactJoin(ctx context.Context, rightTable *TableHandle, on []string, joins []string) (*TableHandle, error) {
	return th.client.exactJoin(ctx, th, rightTable, on, joins)
}

// Performs an as-of-join with this table as the left table.
//
// `on` is the columns to match, which can be a column name or an equals expression (e.g. "colA = colB").
//
// `joins` is the columns to add from the right table.
//
// `matchRule` is the match rule for the join, default is MatchRuleLessThanEqual normally, or MatchRuleGreaterThanEqual or a reverse-as-of-join
func (th *TableHandle) AsOfJoin(ctx context.Context, rightTable *TableHandle, on []string, joins []string, matchRule int) (*TableHandle, error) {
	return th.client.asOfJoin(ctx, th, rightTable, on, joins, matchRule)
}

// Returns a new table containing the first `numRows` rows of each group.
// The columns to group by are selected with the `by` argument.
//
// (See https://deephaven.io/core/groovy/docs/reference/table-operations/group-and-aggregate/headBy/).
func (th *TableHandle) HeadBy(ctx context.Context, numRows int64, by ...string) (*TableHandle, error) {
	return th.client.headOrTailBy(ctx, th, numRows, by, true)
}

// Returns a new table containing the last `numRows` rows of each group.
// The columns to group by are selected with the `by` argument.
//
// (See https://deephaven.io/core/groovy/docs/reference/table-operations/group-and-aggregate/tailBy/).
func (th *TableHandle) TailBy(ctx context.Context, numRows int64, by ...string) (*TableHandle, error) {
	return th.client.headOrTailBy(ctx, th, numRows, by, false)
}

// Performs a group-by aggregation on the table.
// Columns not in the aggregation become array-type.
// If no group-by columns are given, the content of each column is grouped into its own array.
func (th *TableHandle) GroupBy(ctx context.Context, by ...string) (*TableHandle, error) {
	return th.client.dedicatedAggOp(ctx, th, by, "", tablepb2.ComboAggregateRequest_GROUP)
}

// Ungroups the table. The ungroup columns must be arrays.
// `nullFill` indicates whether or not missing cells may be filled with null, default is true
func (th *TableHandle) Ungroup(ctx context.Context, cols []string, nullFill bool) (*TableHandle, error) {
	return th.client.ungroup(ctx, th, cols, nullFill)
}

// Performs a first-by aggregation on the table, i.e. it returns a table that contains the first row of each distinct group.
func (th *TableHandle) FirstBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_FIRST)
}

// Performs a last-by aggregation on the table, i.e. it returns a table that contains the last rrow of each distinct group.
func (th *TableHandle) LastBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_LAST)
}

// Performs a sum-by aggregation on the table. Columns not used in the grouping must be numeric.
func (th *TableHandle) SumBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_SUM)
}

// Performs an absolute-value-sum-by aggregation on the table. Columns not used in the grouping must be numeric.
func (th *TableHandle) AbsSumBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_ABS_SUM)
}

// Performs an average-by aggregation on the table. Columns not used in the grouping must be numeric.
func (th *TableHandle) AvgBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_AVG)
}

// Performs an standard-deviation-by aggregation on the table. Columns not used in the grouping must be numeric.
func (th *TableHandle) StdBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_STD)
}

// Performs an variance-by aggregation on the table. Columns not used in the grouping must be numeric.
func (th *TableHandle) VarBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_VAR)
}

// Performs a median-by aggregation on the table. Columns not used in the grouping must be numeric.
func (th *TableHandle) MedianBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_MEDIAN)
}

// Performs a minimum-by aggregation on the table. Columns not used in the grouping must be numeric.
func (th *TableHandle) MinBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_MIN)
}

// Performs a maximum-by aggregation on the table. Columns not used in the grouping must be numeric.
func (th *TableHandle) MaxBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_MAX)
}

// Performs a count-by aggregation on the table.
// The count of each group is stored in a new column named after the `resultCol` argument
func (th *TableHandle) CountBy(ctx context.Context, resultCol string, cols ...string) (*TableHandle, error) {
	return th.client.dedicatedAggOp(ctx, th, cols, resultCol, tablepb2.ComboAggregateRequest_COUNT)
}

// Counts the number of values in the specified column and returns it as a table with one row and one column
func (th *TableHandle) Count(ctx context.Context, col string) (*TableHandle, error) {
	return th.client.dedicatedAggOp(ctx, th, nil, col, tablepb2.ComboAggregateRequest_COUNT)
}

// Performs an aggregation, grouping by the given columns.
// See the docs on AggBuilder for details on what each of the aggregation types do.
func (th *TableHandle) AggBy(ctx context.Context, agg *AggBuilder, by ...string) (*TableHandle, error) {
	return th.client.aggBy(ctx, th, agg, by)
}

// Combines additional tables into one table by putting their rows on top of each other.
// All tables involved must have the same columns.
// If sortBy is provided, the resulting table will be sorted based on that column.
func (th *TableHandle) Merge(ctx context.Context, sortBy string, others ...*TableHandle) (*TableHandle, error) {
	tables := make([]*TableHandle, len(others)+1)
	tables[0] = th
	copy(tables[1:], others)

	return th.client.merge(ctx, sortBy, tables)
}
