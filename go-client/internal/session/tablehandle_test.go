package session_test

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/deephaven/deephaven-core/go-client/client"
	"github.com/deephaven/deephaven-core/go-client/internal/session"
	"github.com/deephaven/deephaven-core/go-client/internal/test_setup"
)

type unaryTableOp func(context.Context, *session.TableHandle) (session.TableHandle, error)

func applyTableOp(t *testing.T, op unaryTableOp) *array.Record {
	ctx := context.Background()

	c, err := client.NewClient(ctx, "localhost", "10000")
	if err != nil {
		t.Fatalf("NewClient %s", err.Error())
	}
	defer c.Close()

	input := test_setup.ExampleRecord()
	defer input.Release()

	before, err := c.ImportTable(ctx, input)
	if err != nil {
		t.Errorf("ImportTable %s", err.Error())
		return nil
	}

	after, err := op(ctx, &before)
	if err != nil {
		t.Errorf("DropColumns %s", err.Error())
		return nil
	}

	result, err := after.Snapshot(ctx)
	if err != nil {
		t.Errorf("Snapshot %s", err.Error())
		return nil
	}

	return &result
}

func TestDropColumns(t *testing.T) {
	result := applyTableOp(t, func(ctx context.Context, before *session.TableHandle) (session.TableHandle, error) {
		return before.DropColumns(ctx, []string{"Ticker", "Vol"})
	})

	if result != nil {
		rec := *result
		defer rec.Release()

		if rec.NumCols() != 1 {
			t.Errorf("wrong number of columns %d", rec.NumCols())
			return
		}

		if rec.ColumnName(0) != "Close" {
			t.Errorf("wrong column name %s", rec.ColumnName(0))
			return
		}
	}
}

func TestUpdate(t *testing.T) {
	result := applyTableOp(t, func(ctx context.Context, before *session.TableHandle) (session.TableHandle, error) {
		return before.Update(ctx, []string{"Foo = Close * 17.0", "Bar = Vol + 1"})
	})

	if result != nil {
		rec := *result
		defer rec.Release()

		if rec.NumCols() != 5 {
			t.Errorf("wrong number of columns %d", rec.NumCols())
			return
		}

		if rec.ColumnName(3) != "Foo" || rec.ColumnName(4) != "Bar" {
			t.Errorf("wrong column names %s %s", rec.ColumnName(3), rec.ColumnName(4))
			return
		}
	}
}
