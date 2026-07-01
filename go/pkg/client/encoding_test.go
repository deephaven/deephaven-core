package client_test

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/deephaven/deephaven-core/go/internal/test_tools"
	"github.com/deephaven/deephaven-core/go/pkg/client"
)

// Python script that creates REE- and dictionary-encoded tables on the server.
// new_table avoids backtick string literals in Deephaven formulas, which would
// conflict with Go raw string literal delimiters.
//
// ree_table  : 6 rows, Sym = ["a","a","a","b","b","b"]
// dict_table : 5 rows, Sym = ["x","y","z","x","y"]
const encodingSetupScript = `
import jpy
from deephaven import new_table
from deephaven.column import string_col

_JIntCls     = jpy.get_type('org.apache.arrow.vector.types.pojo.ArrowType$Int')
_JUtf8Cls    = jpy.get_type('org.apache.arrow.vector.types.pojo.ArrowType$Utf8')
_JREECls     = jpy.get_type('org.apache.arrow.vector.types.pojo.ArrowType$RunEndEncoded')
_JDictEncCls = jpy.get_type('org.apache.arrow.vector.types.pojo.DictionaryEncoding')
_JField      = jpy.get_type('org.apache.arrow.vector.types.pojo.Field')
_JFieldType  = jpy.get_type('org.apache.arrow.vector.types.pojo.FieldType')
_JSchema     = jpy.get_type('org.apache.arrow.vector.types.pojo.Schema')
_JHashMap    = jpy.get_type('java.util.HashMap')
_JArrayList  = jpy.get_type('java.util.ArrayList')
_JInt32      = _JIntCls(32, True)
_JUtf8       = _JUtf8Cls()
_JREE        = _JREECls.INSTANCE

def _make_ree_field(col_name, val_type, dh_type_str):
    run_ends = _JField.notNullable('run_ends', _JInt32)
    attrs = _JHashMap()
    attrs.put('deephaven:type', dh_type_str)
    val_children = _JArrayList()
    val_f = _JField('values', _JFieldType(True, val_type, None, attrs), val_children)
    children = _JArrayList()
    children.add(run_ends)
    children.add(val_f)
    return _JField(col_name, _JFieldType(True, _JREE, None, None), children)

_ree_src    = new_table([string_col('Sym', ['a', 'a', 'a', 'b', 'b', 'b'])])
_ree_fields = _JArrayList()
_ree_fields.add(_make_ree_field('Sym', _JUtf8, 'java.lang.String'))
_ree_schema = _JSchema(_ree_fields)
ree_table   = _ree_src.with_attributes({'BarrageSchema': _ree_schema})

_dict_src    = new_table([string_col('Sym', ['x', 'y', 'z', 'x', 'y'])])
_dict_enc    = _JDictEncCls(0, False, _JInt32)
_dict_fields = _JArrayList()
_dict_fields.add(_JField('Sym', _JFieldType(True, _JUtf8, _dict_enc, None), _JArrayList()))
_dict_schema = _JSchema(_dict_fields)
dict_table   = _dict_src.with_attributes({'BarrageSchema': _dict_schema})
`

func TestREEEncoding(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(),
		test_tools.GetAuthType(), test_tools.GetAuthToken(), client.WithConsole("python"))
	if err != nil {
		t.Fatalf("NewClient: %s", err.Error())
	}
	defer c.Close()

	if err = c.RunScript(ctx, encodingSetupScript); err != nil {
		t.Fatalf("RunScript: %s", err.Error())
	}

	tbl, err := c.OpenTable(ctx, "ree_table")
	if err != nil {
		t.Fatalf("OpenTable: %s", err.Error())
	}
	defer tbl.Release(ctx)

	rec, err := tbl.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %s", err.Error())
	}
	defer rec.Release()

	if rec.NumRows() != 6 {
		t.Fatalf("expected 6 rows, got %d", rec.NumRows())
	}

	// The column arrives as a RunEndEncoded array; resolve logical indices to physical ones.
	reeArr := rec.Column(0).(*array.RunEndEncoded)
	valArr := reeArr.Values().(*array.String)
	want := []string{"a", "a", "a", "b", "b", "b"}
	for i := 0; i < int(rec.NumRows()); i++ {
		physIdx := reeArr.GetPhysicalIndex(i)
		got := valArr.Value(physIdx)
		if got != want[i] {
			t.Errorf("row %d: got %q, want %q", i, got, want[i])
		}
	}
}

func TestDictionaryEncoding(t *testing.T) {
	ctx := context.Background()

	c, err := client.NewClient(ctx, test_tools.GetHost(), test_tools.GetPort(),
		test_tools.GetAuthType(), test_tools.GetAuthToken(), client.WithConsole("python"))
	if err != nil {
		t.Fatalf("NewClient: %s", err.Error())
	}
	defer c.Close()

	if err = c.RunScript(ctx, encodingSetupScript); err != nil {
		t.Fatalf("RunScript: %s", err.Error())
	}

	tbl, err := c.OpenTable(ctx, "dict_table")
	if err != nil {
		t.Fatalf("OpenTable: %s", err.Error())
	}
	defer tbl.Release(ctx)

	rec, err := tbl.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %s", err.Error())
	}
	defer rec.Release()

	if rec.NumRows() != 5 {
		t.Fatalf("expected 5 rows, got %d", rec.NumRows())
	}

	// The column arrives as a Dictionary array; look up each logical index in the dict values.
	dictArr := rec.Column(0).(*array.Dictionary)
	idxArr := dictArr.Indices().(*array.Int32)
	dictValues := dictArr.Dictionary().(*array.String)
	want := []string{"x", "y", "z", "x", "y"}
	for i := 0; i < int(rec.NumRows()); i++ {
		got := dictValues.Value(int(idxArr.Value(i)))
		if got != want[i] {
			t.Errorf("row %d: got %q, want %q", i, got, want[i])
		}
	}
}
