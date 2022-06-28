package client

import (
	"context"
	"errors"
	"log"

	"github.com/apache/arrow/go/v8/arrow"

	tablepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/table"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"
)

// ErrInvalidTableHandle is returned by most table methods
// when called on a table handle that contains its zero value or has been already released.
var ErrInvalidTableHandle = errors.New("tried to use a zero-value or released table handle")

// A TableHandle is a reference to a table stored on the deephaven server.
//
// It should eventually be released using Release() once it is no longer needed on the client.
// Releasing a table handle does not affect table handles derived from it.
//
// All TableHandle methods (with the exception of Release) are goroutine-safe.
//
// A TableHandle's zero value is an invalid table that cannot be used (it may be released, though, which is a no-op).
type TableHandle struct {
	client   *Client
	ticket   *ticketpb2.Ticket // The ticket this table can be referred to by.
	schema   *arrow.Schema     // The schema (i.e. name, type, and metadata) for this table's columns.
	size     int64             // The number of rows that this table has.
	isStatic bool              // False if this table is dynamic, like a streaming table or a time table.
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

// IsValid returns true if the handle is valid, i.e. table operations can be performed on it.
// No methods can be called on invalid TableHandles except for Release.
func (th *TableHandle) IsValid() bool {
	return th.client != nil
}

// IsStatic returns false for dynamic tables, like streaming tables or time tables.
func (th *TableHandle) IsStatic() bool {
	return th.isStatic
}

// Snapshot downloads the current state of the table from the server and returns it as an Arrow Record.
//
// If a Record is returned successfully, it must be freed later with arrow.record.Release().
func (th *TableHandle) Snapshot(ctx context.Context) (arrow.Record, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.snapshotRecord(ctx, th.ticket)
}

// Query creates a new query based on this table. Table operations can be performed on query nodes to create new nodes.
// A list of query nodes can then be passed to client.ExecQuery() to execute the entire query as a single call.
func (th *TableHandle) Query() QueryNode {
	// TODO: Turn this into a proper error instead of a panic.
	// Returning an error here makes the query API significantly less ergonomic, though.
	// Should the ExecQuery call return the error instead?
	if th.client == nil {
		panic(ErrInvalidTableHandle.Error())
	}

	qb := newQueryBuilder(th.client, th)
	return qb.curRootNode()
}

// Release releases this table handle's resources on the server. The TableHandle is no longer usable after Release is called.
// Ensure that no other methods (including ones in other goroutines) are using this TableHandle.
func (th *TableHandle) Release(ctx context.Context) error {
	if th.client != nil {
		err := th.client.release(ctx, th.ticket)
		if err != nil {
			// This is logged because most of the time this method is used with defer,
			// which will discard the error value.
			log.Println("unable to release table:", err.Error())
			return err
		}

		th.client = nil
		th.ticket = nil
		th.schema = nil
	}
	return nil
}

// DropColumns creates a table with the same number of rows as the source table but omits any columns included in the arguments.
func (th *TableHandle) DropColumns(ctx context.Context, cols ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.dropColumns(ctx, th, cols)
}

// Update creates a new table containing a new, in-memory column for each argument.
// The returned table also includes all the original columns from the source table.
func (th *TableHandle) Update(ctx context.Context, formulas ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.update(ctx, th, formulas)
}

// LazyUpdate creates a new table containing a new, cached, formula column for each argument.
// The returned table also includes all the original columns from the source table.
func (th *TableHandle) LazyUpdate(ctx context.Context, formulas ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.lazyUpdate(ctx, th, formulas)
}

// UpdateView creates a new table containing a new, formula column for each argument.
// When using UpdateView, the new columns are not stored in memory.
// Rather, a formula is stored that is used to recalculate each cell every time it is accessed.
// The returned table also includes all the original columns from the source table.
func (th *TableHandle) UpdateView(ctx context.Context, formulas ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.updateView(ctx, th, formulas)
}

// View creates a new formula table that includes one column for each argument.
// When using view, the data being requested is not stored in memory.
// Rather, a formula is stored that is used to recalculate each cell every time it is accessed.
func (th *TableHandle) View(ctx context.Context, formulas ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.view(ctx, th, formulas)
}

// Select creates a new in-memory table that includes one column for each argument.
// Any columns not specified in the arguments will not appear in the resulting table.
func (th *TableHandle) Select(ctx context.Context, formulas ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.selectTbl(ctx, th, formulas)
}

// SelectDistinct creates a new table containing all of the unique values for a set of key columns.
// When SelectDistinct is used on multiple columns, it looks for distinct sets of values in the selected columns.
func (th *TableHandle) SelectDistinct(ctx context.Context, columns ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.selectDistinct(ctx, th, columns)
}

// Sort returns a new table with rows sorted in a smallest to largest order based on the listed column(s).
func (th *TableHandle) Sort(ctx context.Context, cols ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	var columns []SortColumn
	for _, col := range cols {
		columns = append(columns, SortAsc(col))
	}
	return th.SortBy(ctx, columns...)
}

// Sort returns a new table with rows sorted in the order specified by the listed column(s).
func (th *TableHandle) SortBy(ctx context.Context, cols ...SortColumn) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.sortBy(ctx, th, cols)
}

// Where filters rows of data from the source table.
// It returns a new table with only the rows meeting the filter criteria of the source table.
func (th *TableHandle) Where(ctx context.Context, filters ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.where(ctx, th, filters)
}

// Head returns a table with a specific number of rows from the beginning of the source table.
func (th *TableHandle) Head(ctx context.Context, numRows int64) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.headOrTail(ctx, th, numRows, true)
}

// Tail returns a table with a specific number of rows from the end of the source table.
func (th *TableHandle) Tail(ctx context.Context, numRows int64) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.headOrTail(ctx, th, numRows, false)
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
func (th *TableHandle) NaturalJoin(ctx context.Context, rightTable *TableHandle, on []string, joins []string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.naturalJoin(ctx, th, rightTable, on, joins)
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
// reserveBits is the number of bits of key-space to initially reserve per group, default is 10.
func (th *TableHandle) Join(ctx context.Context, rightTable *TableHandle, on []string, joins []string, reserveBits int32) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.crossJoin(ctx, th, rightTable, on, joins, reserveBits)
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
func (th *TableHandle) ExactJoin(ctx context.Context, rightTable *TableHandle, on []string, joins []string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.exactJoin(ctx, th, rightTable, on, joins)
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
// matchRule is the match rule for the join, default is MatchRuleLessThanEqual normally, or MatchRuleGreaterThanEqual for a reverse-as-of-join
func (th *TableHandle) AsOfJoin(ctx context.Context, rightTable *TableHandle, on []string, joins []string, matchRule MatchRule) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.asOfJoin(ctx, th, rightTable, on, joins, matchRule)
}

// HeadBy returns the first numRows rows for each group.
func (th *TableHandle) HeadBy(ctx context.Context, numRows int64, columnsToGroupBy ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.headOrTailBy(ctx, th, numRows, columnsToGroupBy, true)
}

// TailBy returns the last numRows rows for each group.
func (th *TableHandle) TailBy(ctx context.Context, numRows int64, columnsToGroupBy ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.headOrTailBy(ctx, th, numRows, columnsToGroupBy, false)
}

// GroupBy groups column content into arrays.
// Columns not in the aggregation become array-type.
// If no group-by columns are given, the content of each column is grouped into its own array.
func (th *TableHandle) GroupBy(ctx context.Context, by ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.dedicatedAggOp(ctx, th, by, "", tablepb2.ComboAggregateRequest_GROUP)
}

// Ungroup ungroups column content. It is the inverse of the GroupBy method.
// Ungroup unwraps columns containing either Deephaven arrays or Java arrays.
// nullFill indicates whether or not missing cells may be filled with null, default is true
func (th *TableHandle) Ungroup(ctx context.Context, cols []string, nullFill bool) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.ungroup(ctx, th, cols, nullFill)
}

// FirstBy returns the first row for each group.
// If no columns are given, only the first row of the table is returned.
func (th *TableHandle) FirstBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_FIRST)
}

// LastBy returns the last row for each group.
// If no columns are given, only the last row of the table is returned.
func (th *TableHandle) LastBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_LAST)
}

// SumBy returns the total sum for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (th *TableHandle) SumBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_SUM)
}

// AbsSumBy returns the total sum of absolute values for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (th *TableHandle) AbsSumBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_ABS_SUM)
}

// AvgBy returns the average (mean) of each non-key column for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (th *TableHandle) AvgBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_AVG)
}

// StdBy returns the standard deviation for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (th *TableHandle) StdBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_STD)
}

// VarBy returns the variance for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (th *TableHandle) VarBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_VAR)
}

// MedianBy returns the median value for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (th *TableHandle) MedianBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_MEDIAN)
}

// MinBy returns the minimum value for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (th *TableHandle) MinBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_MIN)
}

// MaxBy returns the maximum value for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (th *TableHandle) MaxBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_MAX)
}

// CountBy returns the number of rows for each group.
// The count of each group is stored in a new column named after the resultCol argument.
func (th *TableHandle) CountBy(ctx context.Context, resultCol string, cols ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.dedicatedAggOp(ctx, th, cols, resultCol, tablepb2.ComboAggregateRequest_COUNT)
}

// Count counts the number of values in the specified column and returns it as a table with one row and one column.
func (th *TableHandle) Count(ctx context.Context, col string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.dedicatedAggOp(ctx, th, nil, col, tablepb2.ComboAggregateRequest_COUNT)
}

// AggBy applies a list of aggregations to table data.
// See the docs on AggBuilder for details on what each of the aggregation types do.
func (th *TableHandle) AggBy(ctx context.Context, agg *AggBuilder, by ...string) (*TableHandle, error) {
	if th.client == nil {
		return nil, ErrInvalidTableHandle
	}
	return th.client.aggBy(ctx, th, agg, by)
}

// Merge combines two or more tables into one aggregate table.
// This essentially appends the tables one on top of the other.
// If sortBy is provided, the resulting table will be sorted based on that column.
func Merge(ctx context.Context, sortBy string, tables ...*TableHandle) (*TableHandle, error) {
	if len(tables) < 1 {
		return nil, errors.New("must provide at least one table to merge")
	}

	for _, table := range tables {
		if !table.IsValid() {
			return nil, ErrInvalidTableHandle
		}
	}

	client := tables[0].client

	return client.merge(ctx, sortBy, nil)
}
