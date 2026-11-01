//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//

using Apache.Arrow;
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class EncodingTest {
  // Python script that creates REE- and dictionary-encoded tables on the server.
  // ree_table     : 6 rows, Sym = ["a","a","a","b","b","b"]
  // dict_table    : 5 rows, Sym = ["x","y","z","x","y"]
  // reedict_table : 6 rows, Sym = ["a","a","a","b","b","b"], doubly-encoded RunEndEncoded<Dictionary<...>>
  private const string EncodingSetupScript =
    """
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
    ree_table   = _ree_src.with_attributes({'BarrageSchema': _ree_schema
    })

    _dict_src    = new_table([string_col('Sym', ['x', 'y', 'z', 'x', 'y'])])
    _dict_enc    = _JDictEncCls(0, False, _JInt32)
    _dict_fields = _JArrayList()
    _dict_fields.add(_JField('Sym', _JFieldType(True, _JUtf8, _dict_enc, None), _JArrayList()))
    _dict_schema = _JSchema(_dict_fields)
    dict_table   = _dict_src.with_attributes({'BarrageSchema': _dict_schema})

    def _make_ree_dict_field(col_name, val_type, dh_type_str, dict_id):
        run_ends = _JField.notNullable('run_ends', _JInt32)
        attrs = _JHashMap()
        attrs.put('deephaven:type', dh_type_str)
        dict_enc = _JDictEncCls(dict_id, False, _JInt32)
        val_children = _JArrayList()
        val_f = _JField('values', _JFieldType(True, val_type, dict_enc, attrs), val_children)
        children = _JArrayList()
        children.add(run_ends)
        children.add(val_f)
        return _JField(col_name, _JFieldType(True, _JREE, None, None), children)

    _reedict_src = new_table([string_col('Sym', ['a', 'a', 'a', 'b', 'b', 'b'])])
    _reedict_fields = _JArrayList()
    _reedict_fields.add(_make_ree_dict_field('Sym', _JUtf8, 'java.lang.String', 0))
    _reedict_schema = _JSchema(_reedict_fields)
    reedict_table = _reedict_src.with_attributes({ 'BarrageSchema': _reedict_schema})
    """;

  // A dictionary-encoded column that is null for every row of the snapshot's first batch. Snapshots
  // start with a 4096-row batch and grow from there, so the server's isDelta=false base dictionary
  // for that batch is empty and the values in later batches arrive as a dictionary delta, which
  // Apache.Arrow's reader cannot concatenate onto an empty dictionary without the workaround in
  // FlightIpcReader. The row counts are kept in sync with the script below.
  // nulls_first_dict_table : 100000 rows, Sym = null for ii < 50000, otherwise "v" + (ii % 3)
  private const int NullsFirstNumRows = 100_000;
  private const int NullsFirstNumLeadingNulls = 50_000;
  private const string NullsFirstSetupScript =
    """
    import jpy
    from deephaven import empty_table

    _JIntCls     = jpy.get_type('org.apache.arrow.vector.types.pojo.ArrowType$Int')
    _JUtf8Cls    = jpy.get_type('org.apache.arrow.vector.types.pojo.ArrowType$Utf8')
    _JDictEncCls = jpy.get_type('org.apache.arrow.vector.types.pojo.DictionaryEncoding')
    _JField      = jpy.get_type('org.apache.arrow.vector.types.pojo.Field')
    _JFieldType  = jpy.get_type('org.apache.arrow.vector.types.pojo.FieldType')
    _JSchema     = jpy.get_type('org.apache.arrow.vector.types.pojo.Schema')
    _JArrayList  = jpy.get_type('java.util.ArrayList')
    _JInt32      = _JIntCls(32, True)
    _JUtf8       = _JUtf8Cls()

    _nf_src    = empty_table(100000).update(['Sym = ii < 50000 ? null : (`v` + (ii % 3))'])
    _nf_enc    = _JDictEncCls(0, False, _JInt32)
    _nf_fields = _JArrayList()
    _nf_fields.add(_JField('Sym', _JFieldType(True, _JUtf8, _nf_enc, None), _JArrayList()))
    nulls_first_dict_table = _nf_src.with_attributes({'BarrageSchema': _JSchema(_nf_fields)})
    """;

  [Test]
  public async Task RunEndEncodedTable() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());

    var thm = ctx.Client.Manager;
    thm.RunScript(EncodingSetupScript);
    using var actual = thm.FetchTable("ree_table");

    var expected = new TableMaker();
    expected.AddColumn("Sym", ["a", "a", "a", "b", "b", "b"]);

    TableComparer.AssertSame(expected, actual);
  }

  [Test]
  public async Task DictionaryEncodedTable() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());

    var thm = ctx.Client.Manager;
    thm.RunScript(EncodingSetupScript);
    using var actual = thm.FetchTable("dict_table");

    var expected = new TableMaker();
    expected.AddColumn("Sym", ["x", "y", "z", "x", "y"]);
    TableComparer.AssertSame(expected, actual);
  }

  [Test]
  public async Task RunEndPlusDictionaryEncodedTable() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());

    var thm = ctx.Client.Manager;
    thm.RunScript(EncodingSetupScript);
    using var actual = thm.FetchTable("reedict_table");

    var expected = new TableMaker();
    expected.AddColumn("Sym", ["a", "a", "a", "b", "b", "b"]);
    TableComparer.AssertSame(expected, actual);
  }

  [Test]
  public async Task DictionaryColumnNullThroughoutFirstBatchIsFetchedCorrectly() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;

    thm.RunScript(NullsFirstSetupScript);
    using var t = thm.FetchTable("nulls_first_dict_table");

    var syms = new List<string?>();
    for (var i = 0; i != NullsFirstNumRows; ++i) {
      syms.Add(i < NullsFirstNumLeadingNulls ? null : $"v{i % 3}");
    }
    var expected = new TableMaker();
    expected.AddColumn("Sym", syms);
    await Assert.That(() => TableComparer.AssertSame(expected, t)).ThrowsNothing();
  }
}
