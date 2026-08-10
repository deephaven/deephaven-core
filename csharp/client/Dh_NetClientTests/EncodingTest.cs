//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
using Deephaven.Dh_NetClient;

namespace Deephaven.Dh_NetClientTests;

public class EncodingTest {
  // Shared preamble for the setup scripts below: the Arrow pojo classes needed to describe an
  // encoded column, plus the field builders for the two run-end-encoded flavors. A column is
  // encoded by handing the server a BarrageSchema attribute whose field of the same name carries
  // the desired encoding; columns absent from that schema keep their natural (unencoded) type.
  private const string EncodingPreamble = """
    import jpy
    from deephaven import new_table, time_table
    from deephaven.column import string_col

    _JIntCls      = jpy.get_type('org.apache.arrow.vector.types.pojo.ArrowType$Int')
    _JREECls      = jpy.get_type('org.apache.arrow.vector.types.pojo.ArrowType$RunEndEncoded')
    _JDictEncCls  = jpy.get_type('org.apache.arrow.vector.types.pojo.DictionaryEncoding')
    _JField       = jpy.get_type('org.apache.arrow.vector.types.pojo.Field')
    _JFieldType   = jpy.get_type('org.apache.arrow.vector.types.pojo.FieldType')
    _JSchema      = jpy.get_type('org.apache.arrow.vector.types.pojo.Schema')
    _JHashMap     = jpy.get_type('java.util.HashMap')
    _JArrayList   = jpy.get_type('java.util.ArrayList')
    _JBarrageUtil = jpy.get_type('io.deephaven.extensions.barrage.util.BarrageUtil')
    _JInt32       = _JIntCls(32, True)
    _JREE         = _JREECls.INSTANCE

    # A BarrageSchema attribute is the authoritative wire schema: its field list determines both the
    # set and the order of the columns the server sends, not merely which of them are encoded. It must
    # therefore describe every column of the table -- naming only the encoded column projects the rest
    # away, which then desynchronizes the server's per-column chunk writers. So start from the table's
    # natural schema and replace exactly one field.
    def _encoded_schema(table, col_name, to_encoded_field):
        natural = _JBarrageUtil.makeSchema(
            _JBarrageUtil.DEFAULT_SNAPSHOT_OPTIONS, table.j_table.getDefinition(), _JHashMap(), False)
        natural_fields = natural.getFields()
        fields = _JArrayList()
        for i in range(natural_fields.size()):
            f = natural_fields.get(i)
            fields.add(to_encoded_field(f) if f.getName() == col_name else f)
        return _JSchema(fields)

    # The three field transforms. Each keeps the natural field's value type and metadata (which already
    # carries deephaven:type) and only adds the encoding. The run-end-encoded parent is non-nullable:
    # an REE array carries no validity buffer of its own, nulls live on the values child.
    def _to_ree_field(f):
        run_ends = _JField.notNullable('run_ends', _JInt32)
        values = _JField('values', _JFieldType(f.isNullable(), f.getType(), None, f.getMetadata()),
                         f.getChildren())
        children = _JArrayList()
        children.add(run_ends)
        children.add(values)
        return _JField(f.getName(), _JFieldType(False, _JREE, None, f.getMetadata()), children)

    def _to_dict_field(f):
        dict_enc = _JDictEncCls(0, False, _JInt32)
        return _JField(f.getName(),
                       _JFieldType(f.isNullable(), f.getType(), dict_enc, f.getMetadata()),
                       f.getChildren())

    def _to_ree_dict_field(f):
        run_ends = _JField.notNullable('run_ends', _JInt32)
        dict_enc = _JDictEncCls(0, False, _JInt32)
        values = _JField('values', _JFieldType(f.isNullable(), f.getType(), dict_enc, f.getMetadata()),
                         f.getChildren())
        children = _JArrayList()
        children.add(run_ends)
        children.add(values)
        return _JField(f.getName(), _JFieldType(False, _JREE, None, f.getMetadata()), children)

    """;

  // Static (snapshot) tables.
  // ree_table     : 6 rows, Sym = ["a","a","a","b","b","b"]
  // dict_table    : 5 rows, Sym = ["x","y","z","x","y"]
  // reedict_table : 6 rows, Sym = ["a","a","a","b","b","b"], doubly-encoded RunEndEncoded<Dictionary<...>>
  private const string StaticEncodingTables = """
    _ree_src   = new_table([string_col('Sym', ['a', 'a', 'a', 'b', 'b', 'b'])])
    ree_table  = _ree_src.with_attributes(
        {'BarrageSchema': _encoded_schema(_ree_src, 'Sym', _to_ree_field)})

    _dict_src  = new_table([string_col('Sym', ['x', 'y', 'z', 'x', 'y'])])
    dict_table = _dict_src.with_attributes(
        {'BarrageSchema': _encoded_schema(_dict_src, 'Sym', _to_dict_field)})

    _reedict_src   = new_table([string_col('Sym', ['a', 'a', 'a', 'b', 'b', 'b'])])
    reedict_table  = _reedict_src.with_attributes(
        {'BarrageSchema': _encoded_schema(_reedict_src, 'Sym', _to_ree_dict_field)})

    """;

  // Ticking tables. Sym is deliberately shaped to stress all three encodings while staying a pure
  // function of row position:
  //   - runs of three identical values, so REE has multi-row runs to expand
  //   - a brand-new distinct value every three rows, so the dictionary keeps growing and the server
  //     must ship isDelta=true DictionaryBatch messages on nearly every update rather than one
  //     complete dictionary up front (the case a snapshot test cannot reach)
  //   - a null every seventh row, which breaks runs and produces null dictionary indices; it also
  //     makes the same dictionary value appear in two non-adjacent runs (e.g. sym3 at ii 9 and 11)
  // Row ii always holds II == ii and Sym == (ii % 7 == 3 ? null : "sym" + ii / 3), so the client can
  // compute the expected contents of the whole table from its row count alone. Note the (long) cast
  // on the division: the query language's '/' is floating-point division even for integral operands,
  // so without it every row would get a distinct fractional value instead of runs of three.
  private const string TickingEncodingTables = """
    _ticking_src = (time_table('PT0.1S')
                    .update(['II = ii', 'Sym = (ii % 7 == 3) ? null : (`sym` + (long)(ii / 3))'])
                    .drop_columns(['Timestamp']))

    # Each of these is an attribute-only copy of the one source above, so all three tests see
    # identical data and differ only in how the server is asked to encode Sym.
    ree_ticking_table = _ticking_src.with_attributes(
        {'BarrageSchema': _encoded_schema(_ticking_src, 'Sym', _to_ree_field)})

    dict_ticking_table = _ticking_src.with_attributes(
        {'BarrageSchema': _encoded_schema(_ticking_src, 'Sym', _to_dict_field)})

    reedict_ticking_table = _ticking_src.with_attributes(
        {'BarrageSchema': _encoded_schema(_ticking_src, 'Sym', _to_ree_dict_field)})

    """;

  private const string StaticSetupScript = EncodingPreamble + StaticEncodingTables;
  private const string TickingSetupScript = EncodingPreamble + TickingEncodingTables;

  // The number of rows to wait for before declaring a ticking test finished. At the script's tick
  // interval this spans several Barrage updates, so the dictionary grows across message boundaries
  // rather than arriving complete in the initial snapshot.
  private const Int64 TargetRows = 30;

  [Test]
  public async Task RunEndEncodedTableIsFetchedAndDecodedCorrectly() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;

    thm.RunScript(StaticSetupScript);
    using var t = thm.FetchTable("ree_table");

    var expected = new TableMaker();
    expected.AddColumn("Sym", new[] { "a", "a", "a", "b", "b", "b" });
    await Assert.That(() => TableComparer.AssertSame(expected, t)).ThrowsNothing();
  }

  [Test]
  public async Task DictionaryEncodedTableIsFetchedAndDecodedCorrectly() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;

    thm.RunScript(StaticSetupScript);
    using var t = thm.FetchTable("dict_table");

    var expected = new TableMaker();
    expected.AddColumn("Sym", new[] { "x", "y", "z", "x", "y" });
    await Assert.That(() => TableComparer.AssertSame(expected, t)).ThrowsNothing();
  }

  [Test]
  public async Task RunEndPlusDictionaryEncodedTableIsFetchedAndDecodedCorrectly() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;

    thm.RunScript(StaticSetupScript);
    using var t = thm.FetchTable("reedict_table");

    var expected = new TableMaker();
    expected.AddColumn("Sym", new[] { "a", "a", "a", "b", "b", "b" });
    await Assert.That(() => TableComparer.AssertSame(expected, t)).ThrowsNothing();
  }

  [Test]
  public async Task TickingRunEndEncodedTableIsDecodedCorrectly() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;

    thm.RunScript(TickingSetupScript);
    await SubscribeAndValidate(thm, "ree_ticking_table");
  }

  [Test]
  public async Task TickingDictionaryEncodedTableIsDecodedCorrectly() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;

    thm.RunScript(TickingSetupScript);
    await SubscribeAndValidate(thm, "dict_ticking_table");
  }

  [Test]
  public async Task TickingRunEndPlusDictionaryTableIsDecodedCorrectly() {
    using var ctx = CommonContextForTests.Create(new ClientOptions());
    var thm = ctx.Client.Manager;

    thm.RunScript(TickingSetupScript);
    await SubscribeAndValidate(thm, "reedict_ticking_table");
  }

  /// <summary>
  /// Subscribes to the named ticking table and validates every update until the table reaches
  /// TargetRows rows.
  /// </summary>
  private static async Task SubscribeAndValidate(TableHandleManager thm, string tableName) {
    using var table = thm.FetchTable(tableName);
    var callback = new EncodedTickingCallback(TargetRows);
    using var cookie = table.Subscribe(callback);

    while (true) {
      var (done, exception) = await callback.WaitForUpdateAsync();
      if (exception != null) {
        throw new Exception(exception.Message, exception);
      }
      if (done) {
        break;
      }
    }
  }

  /// <summary>
  /// The Sym value the ticking setup script assigns to the row at position 'index'. Kept in sync
  /// with the Sym formula in TickingEncodingTables.
  /// </summary>
  private static string? ExpectedSym(Int64 index) {
    return index % 7 == 3 ? null : $"sym{index / 3}";
  }

  /// <summary>
  /// Ticking callback for the encoded-column tests. On every update it recomputes the expected
  /// contents of the entire table from the current row count and compares. Validating on each tick,
  /// rather than only once at the end, means a bad dictionary delta or a mis-expanded run is caught
  /// on the update that carries it. Finishes once the table has reached 'targetRows' rows.
  ///
  /// A mismatch throws out of OnNext, which the subscription machinery reports through OnError;
  /// the test body then rethrows it on the main thread.
  /// </summary>
  private sealed class EncodedTickingCallback(Int64 targetRows) : CommonBase {
    public override void OnNext(TickingUpdate update) {
      var current = update.Current;
      var numRows = current.NumRows;
      // Progress trace: if the test dies, the last line recorded tells us which update it died on.
      Console.WriteLine($"=== update: {numRows} rows ===");
      if (numRows == 0) {
        // The initial snapshot of a fresh time table can be empty.
        return;
      }

      var iis = new List<Int64>();
      var syms = new List<string?>();
      for (Int64 i = 0; i != numRows; ++i) {
        iis.Add(i);
        syms.Add(ExpectedSym(i));
      }

      var expected = new TableMaker();
      expected.AddColumn("II", iis);
      expected.AddColumn("Sym", syms);
      TableComparer.AssertSame(expected, current);

      if (numRows >= targetRows) {
        NotifyDone();
      }
    }
  }
}
