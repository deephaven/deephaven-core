package client

import (
	"context"
	"errors"
	"log"
	"sync"

	"github.com/apache/arrow/go/v8/arrow"

	tablepb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/table"
	ticketpb2 "github.com/deephaven/deephaven-core/go-client/internal/proto/ticket"
)

// ErrInvalidTableHandle is returned by most table methods
// when called on a table handle that contains its zero value or has been already released.
var ErrInvalidTableHandle = errors.New("tried to use a nil, zero-value, or released table handle")

// ErrDifferentClients is returned when performing a table operation
// on handles that come from different Client structs.
var ErrDifferentClients = errors.New("tried to use tables from different clients")

// A TableHandle is a reference to a table stored on the deephaven server.
//
// It should eventually be released using Release() once it is no longer needed on the client.
// Releasing a table handle does not affect table handles derived from it.
// Once a TableHandle has been released, no other methods should be called on it.
//
// All TableHandle methods are goroutine-safe.
//
// A TableHandle's zero value acts identically to a TableHandle that has been released.
// A nil TableHandle pointer also acts like a released table with one key exception:
// The Merge and MergeQuery methods will simply ignore nil handles.
type TableHandle struct {
	client   *Client
	ticket   *ticketpb2.Ticket // The ticket this table can be referred to by.
	schema   *arrow.Schema     // The schema (i.e. name, type, and metadata) for this table's columns.
	size     int64             // The number of rows that this table has. Not meaningful if isStatic is false.
	isStatic bool              // False if this table is dynamic, like a streaming table or a time table.

	lock sync.RWMutex // Used to guard the state of the table handle. Table operations acquire a read lock, and releasing the table acquires a write lock.
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
	if th == nil {
		return false
	}
	th.lock.RLock()
	defer th.lock.RUnlock()
	return th.client != nil
}

// rLockIfValid returns true if the handle is valid, i.e. table operations can be performed on it.
// If this function returns true, it will acquire a read lock for the handle.
func (th *TableHandle) rLockIfValid() bool {
	if th == nil {
		return false
	}
	th.lock.RLock()
	if th.client == nil {
		th.lock.RUnlock()
		return false
	}
	return true
}

// IsStatic returns false for dynamic tables, like streaming tables or time tables.
func (th *TableHandle) IsStatic() bool {
	// No need to lock since this is never changed
	return th.isStatic
}

// NumRows returns the number of rows in the table.
// The return value is only ok if IsStatic() is true,
// since only static tables have a fixed number of rows.
func (th *TableHandle) NumRows() (numRows int64, ok bool) {
	// No need to lock since these are never changed
	if th.isStatic {
		return th.size, true
	} else {
		// Return -1 here so that it can't possibly be mistaken for a valid size
		return -1, false
	}
}

// Snapshot downloads the current state of the table from the server and returns it as an Arrow Record.
//
// If a Record is returned successfully, it must be freed later with arrow.record.Release().
func (th *TableHandle) Snapshot(ctx context.Context) (arrow.Record, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.snapshotRecord(ctx, th.ticket)
}

// Query creates a new query based on this table. Table operations can be performed on query nodes to create new nodes.
// A list of query nodes can then be passed to client.ExecSerial() or client.ExecBatch() to get a list of tables.
func (th *TableHandle) Query() QueryNode {
	// The validity check and lock will occur when the query is actually used, so they aren't handled here.
	qb := newQueryBuilder(th)
	return qb.curRootNode()
}

// Release releases this table handle's resources on the server. The TableHandle is no longer usable after Release is called.
// It is safe to call Release multiple times.
func (th *TableHandle) Release(ctx context.Context) error {
	if th == nil {
		return nil
	}

	th.lock.Lock()
	defer th.lock.Unlock()

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
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.dropColumns(ctx, th, cols)
}

// Update creates a new table containing a new, in-memory column for each argument.
// The returned table also includes all the original columns from the source table.
func (th *TableHandle) Update(ctx context.Context, formulas ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.update(ctx, th, formulas)
}

// LazyUpdate creates a new table containing a new, cached, formula column for each argument.
// The returned table also includes all the original columns from the source table.
func (th *TableHandle) LazyUpdate(ctx context.Context, formulas ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.lazyUpdate(ctx, th, formulas)
}

// UpdateView creates a new table containing a new, formula column for each argument.
// When using UpdateView, the new columns are not stored in memory.
// Rather, a formula is stored that is used to recalculate each cell every time it is accessed.
// The returned table also includes all the original columns from the source table.
func (th *TableHandle) UpdateView(ctx context.Context, formulas ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.updateView(ctx, th, formulas)
}

// View creates a new formula table that includes one column for each argument.
// When using view, the data being requested is not stored in memory.
// Rather, a formula is stored that is used to recalculate each cell every time it is accessed.
func (th *TableHandle) View(ctx context.Context, formulas ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.view(ctx, th, formulas)
}

// Select creates a new in-memory table that includes one column for each argument.
// Any columns not specified in the arguments will not appear in the resulting table.
func (th *TableHandle) Select(ctx context.Context, formulas ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.selectTbl(ctx, th, formulas)
}

// SelectDistinct creates a new table containing all of the unique values for a set of key columns.
// When SelectDistinct is used on multiple columns, it looks for distinct sets of values in the selected columns.
func (th *TableHandle) SelectDistinct(ctx context.Context, columns ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.selectDistinct(ctx, th, columns)
}

// Sort returns a new table with rows sorted in a smallest to largest order based on the listed column(s).
func (th *TableHandle) Sort(ctx context.Context, cols ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	var columns []SortColumn
	for _, col := range cols {
		columns = append(columns, SortAsc(col))
	}
	return th.SortBy(ctx, columns...)
}

// Sort returns a new table with rows sorted in the order specified by the listed column(s).
func (th *TableHandle) SortBy(ctx context.Context, cols ...SortColumn) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.sortBy(ctx, th, cols)
}

// Where filters rows of data from the source table.
// It returns a new table with only the rows meeting the filter criteria of the source table.
func (th *TableHandle) Where(ctx context.Context, filters ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.where(ctx, th, filters)
}

// Head returns a table with a specific number of rows from the beginning of the source table.
func (th *TableHandle) Head(ctx context.Context, numRows int64) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.headOrTail(ctx, th, numRows, true)
}

// Tail returns a table with a specific number of rows from the end of the source table.
func (th *TableHandle) Tail(ctx context.Context, numRows int64) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
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
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	if !rightTable.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer rightTable.lock.RUnlock()

	// Different-client check is done by this method.
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
// reserveBits is the number of bits of key-space to initially reserve per group. Set it to 10 if unsure.
func (th *TableHandle) Join(ctx context.Context, rightTable *TableHandle, on []string, joins []string, reserveBits int32) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	if !rightTable.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer rightTable.lock.RUnlock()

	// Different-client check is done by this method.
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
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	if !rightTable.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer rightTable.lock.RUnlock()

	// Different-client check is done by this method.
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
// matchRule is the match rule for the join.
// Use MatchRuleLessThanEqual for a normal as-of join, or MatchRuleGreaterThanEqual for a reverse-as-of-join.
func (th *TableHandle) AsOfJoin(ctx context.Context, rightTable *TableHandle, on []string, joins []string, matchRule MatchRule) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	if !rightTable.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer rightTable.lock.RUnlock()

	// Different-client check is done by this method.
	return th.client.asOfJoin(ctx, th, rightTable, on, joins, matchRule)
}

// HeadBy returns the first numRows rows for each group.
func (th *TableHandle) HeadBy(ctx context.Context, numRows int64, columnsToGroupBy ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.headOrTailBy(ctx, th, numRows, columnsToGroupBy, true)
}

// TailBy returns the last numRows rows for each group.
func (th *TableHandle) TailBy(ctx context.Context, numRows int64, columnsToGroupBy ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.headOrTailBy(ctx, th, numRows, columnsToGroupBy, false)
}

// GroupBy groups column content into arrays.
// Columns not in the aggregation become array-type.
// If no group-by columns are given, the content of each column is grouped into its own array.
func (th *TableHandle) GroupBy(ctx context.Context, by ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.dedicatedAggOp(ctx, th, by, "", tablepb2.ComboAggregateRequest_GROUP)
}

// Ungroup ungroups column content. It is the inverse of the GroupBy method.
// Ungroup unwraps columns containing either Deephaven arrays or Java arrays.
// nullFill indicates whether or not missing cells may be filled with null. Set it to true if unsure.
func (th *TableHandle) Ungroup(ctx context.Context, cols []string, nullFill bool) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.ungroup(ctx, th, cols, nullFill)
}

// FirstBy returns the first row for each group.
// If no columns are given, only the first row of the table is returned.
func (th *TableHandle) FirstBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_FIRST)
}

// LastBy returns the last row for each group.
// If no columns are given, only the last row of the table is returned.
func (th *TableHandle) LastBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_LAST)
}

// SumBy returns the total sum for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (th *TableHandle) SumBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_SUM)
}

// AbsSumBy returns the total sum of absolute values for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (th *TableHandle) AbsSumBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_ABS_SUM)
}

// AvgBy returns the average (mean) of each non-key column for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (th *TableHandle) AvgBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_AVG)
}

// StdBy returns the standard deviation for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (th *TableHandle) StdBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_STD)
}

// VarBy returns the variance for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (th *TableHandle) VarBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_VAR)
}

// MedianBy returns the median value for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (th *TableHandle) MedianBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_MEDIAN)
}

// MinBy returns the minimum value for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (th *TableHandle) MinBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_MIN)
}

// MaxBy returns the maximum value for each group. Null values are ignored.
// Columns not used in the grouping must be numeric.
func (th *TableHandle) MaxBy(ctx context.Context, cols ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.dedicatedAggOp(ctx, th, cols, "", tablepb2.ComboAggregateRequest_MAX)
}

// CountBy returns the number of rows for each group.
// The count of each group is stored in a new column named after the resultCol argument.
func (th *TableHandle) CountBy(ctx context.Context, resultCol string, cols ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.dedicatedAggOp(ctx, th, cols, resultCol, tablepb2.ComboAggregateRequest_COUNT)
}

// Count counts the number of values in the specified column and returns it as a table with one row and one column.
func (th *TableHandle) Count(ctx context.Context, col string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.dedicatedAggOp(ctx, th, nil, col, tablepb2.ComboAggregateRequest_COUNT)
}

// AggBy applies a list of aggregations to table data.
// See the docs on AggBuilder for details on what each of the aggregation types do.
func (th *TableHandle) AggBy(ctx context.Context, agg *AggBuilder, by ...string) (*TableHandle, error) {
	if !th.rLockIfValid() {
		return nil, ErrInvalidTableHandle
	}
	defer th.lock.RUnlock()
	return th.client.aggBy(ctx, th, agg.aggs, by)
}

// Merge combines two or more tables into one table.
// This essentially appends the tables on top of each other.
//
// If sortBy is provided, the resulting table will be sorted based on that column.
//
// Any nil TableHandle pointers passed in are ignored.
// At least one non-nil *TableHandle must be provided.
func Merge(ctx context.Context, sortBy string, tables ...*TableHandle) (*TableHandle, error) {
	// First, remove all the nil TableHandles.
	// No lock needed here since we're not using any TableHandle methods.
	var usedTables []*TableHandle
	for _, table := range tables {
		if table != nil {
			usedTables = append(usedTables, table)
		}
	}

	if len(usedTables) < 1 {
		return nil, errors.New("must provide at least one non-nil table to merge")
	}

	for _, table := range usedTables {
		if !table.rLockIfValid() {
			return nil, ErrInvalidTableHandle
		}
		defer table.lock.RUnlock()
	}

	client := usedTables[0].client

	return client.merge(ctx, sortBy, usedTables)
}
