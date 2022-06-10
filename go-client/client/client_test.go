package client_test

import (
	"context"
	"testing"

	"github.com/deephaven/deephaven-core/go-client/client"
	"github.com/deephaven/deephaven-core/go-client/internal/test_setup"
)

func TestConnectError(t *testing.T) {
	ctx := context.Background()

	_, err := client.NewClient(ctx, "localhost", "1234", "python")
	if err == nil {
		t.Fatalf("client did not fail to connect")
	}
}

func TestClosedClient(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, "localhost", "10000", "python")
	if err != nil {
		t.Fatalf("NewClient err %s", err.Error())
	}

	c.Close()

	_, err = c.EmptyTable(ctx, 17)
	if err == nil {
		t.Error("client did not close")
	}

	// Multiple times should be OK
	c.Close()
}

func TestEmptyTable(t *testing.T) {
	var expectedRows int64 = 5
	var expectedCols int64 = 0

	ctx := context.Background()

	c, err := client.NewClient(ctx, "localhost", "10000", "python")
	if err != nil {
		t.Fatalf("NewClient err %s", err.Error())
	}
	defer c.Close()

	tbl, err := c.EmptyTable(ctx, expectedRows)
	if err != nil {
		t.Errorf("EmptyTable err %s", err.Error())
	}

	rec, err := tbl.Snapshot(ctx)
	if err != nil {
		t.Errorf("Snapshot err %s", err.Error())
	}
	defer rec.Release()

	rows, cols := rec.NumRows(), rec.NumCols()
	if rows != expectedRows || cols != expectedCols {
		t.Errorf("Record had wrong size (expected %d x %d, got %d x %d)", expectedRows, expectedCols, rows, cols)
	}
}

func TestTimeTable(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, "localhost", "10000", "python")
	if err != nil {
		t.Fatalf("NewClient err %s", err.Error())
	}
	defer c.Close()

	tbl, err := c.TimeTable(ctx, 10000000, nil)
	if err != nil {
		t.Errorf("EmptyTable err %s", err.Error())
	}

	if tbl.IsStatic() {
		t.Error("time table should not be static")
		return
	}
}

func TestTableUpload(t *testing.T) {
	r := test_setup.ExampleRecord()
	defer r.Release()

	ctx := context.Background()
	s, err := client.NewClient(ctx, "localhost", "10000", "python")
	if err != nil {
		t.Fatalf("NewClient err %s", err.Error())
	}
	defer s.Close()

	tbl, err := s.ImportTable(ctx, r)
	if err != nil {
		t.Errorf("ImportTable err %s", err.Error())
	}

	rec, err := tbl.Snapshot(ctx)
	if err != nil {
		t.Errorf("Snapshot err %s", err.Error())
	}
	defer rec.Release()

	if r.NumRows() != rec.NumRows() || r.NumCols() != rec.NumCols() {
		t.Log("Expected:")
		t.Log(r)
		t.Log("Actual:")
		t.Log(rec)
		t.Errorf("uploaded and snapshotted table differed (%d x %d vs %d x %d)", r.NumRows(), r.NumCols(), rec.NumRows(), rec.NumCols())
		return
	}

	for col := 0; col < int(r.NumCols()); col += 1 {
		expCol := r.Column(col)
		actCol := rec.Column(col)

		if expCol.DataType() != actCol.DataType() {
			t.Error("DataType differed", expCol.DataType(), " and ", actCol.DataType())
		}
	}
}
