package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/deephaven/deephaven-core/go-client/client"
	"github.com/deephaven/deephaven-core/go-client/internal/test_tools"
)

func TestConnectError(t *testing.T) {
	ctx := context.Background()

	_, err := client.NewClient(ctx, test_tools.GetHost(), "1234", "python")
	if err == nil {
		t.Fatalf("client did not fail to connect")
	}
}

func TestClosedClient(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
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

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
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

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
	if err != nil {
		t.Fatalf("NewClient err %s", err.Error())
	}
	defer c.Close()

	tbl, err := c.TimeTable(ctx, 10000000, time.Now())
	if err != nil {
		t.Errorf("EmptyTable err %s", err.Error())
	}

	if tbl.IsStatic() {
		t.Error("time table should not be static")
		return
	}
}

func TestTableUpload(t *testing.T) {
	r := test_tools.ExampleRecord()
	defer r.Release()

	ctx := context.Background()
	s, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
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

func contains(slice []string, elem string) bool {
	for _, e := range slice {
		if e == elem {
			return true
		}
	}
	return false
}

func TestFieldSyncOnce(t *testing.T) {
	ctx := context.Background()

	client1, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
	test_tools.CheckError(t, "NewClient", err)
	defer client1.Close()

	err = client1.RunScript(ctx,
		`
gotesttable = None
`)
	test_tools.CheckError(t, "RunScript", err)

	client2, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
	test_tools.CheckError(t, "NewClient", err)
	defer client2.Close()

	err = client2.FetchTables(ctx, client.FetchOnce)
	test_tools.CheckError(t, "FetchTables", err)

	if contains(client2.ListOpenableTables(), "gotesttable") {
		t.Errorf("test table should not exist")
		return
	}

	err = client1.RunScript(ctx,
		`
from deephaven import empty_table
gotesttable = empty_table(10)
`)
	test_tools.CheckError(t, "RunScript", err)

	err = client2.FetchTables(ctx, client.FetchOnce)
	test_tools.CheckError(t, "FetchTables", err)

	if !contains(client2.ListOpenableTables(), "gotesttable") {
		t.Errorf("test table should exist")
		return
	}
}

func TestFieldSyncRepeating(t *testing.T) {
	ctx := context.Background()

	client1, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
	test_tools.CheckError(t, "NewClient", err)
	defer client1.Close()

	err = client1.RunScript(ctx,
		`
gotesttable1 = None
`)
	test_tools.CheckError(t, "RunScript", err)

	client2, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(), "python")
	test_tools.CheckError(t, "NewClient", err)
	defer client2.Close()

	err = client2.FetchTables(ctx, client.FetchRepeating)
	test_tools.CheckError(t, "FetchTables", err)

	err = client1.RunScript(ctx,
		`
from deephaven import empty_table
gotesttable1 = empty_table(10)
`)
	test_tools.CheckError(t, "RunScript", err)

	timer := time.After(time.Second)
	for {
		if contains(client2.ListOpenableTables(), "gotesttable1") {
			break
		}

		select {
		case <-timer:
			t.Errorf("test table should exist")
			return
		default:
		}
	}

	err = client2.RunScript(ctx, "print('hi')")
	test_tools.CheckError(t, "RunScript", err)

	client1.RunScript(ctx,
		`
from deephaven import empty_table
gotesttable2 = empty_table(20)
`)
	test_tools.CheckError(t, "RunScript", err)

	timer = time.After(time.Second)
	for {
		if contains(client2.ListOpenableTables(), "gotesttable1") && contains(client2.ListOpenableTables(), "gotesttable2") {
			break
		}

		select {
		case <-timer:
			t.Errorf("test tables should exist")
			return
		default:
		}
	}

	tbl, err := client2.OpenTable(ctx, "gotesttable1")
	test_tools.CheckError(t, "OpenTable", err)
	defer tbl.Release(ctx)
}
