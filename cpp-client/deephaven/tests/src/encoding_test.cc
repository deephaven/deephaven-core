/*
 * Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
 */
#include <string>
#include <vector>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"

using deephaven::client::utility::TableMaker;

namespace deephaven::client::tests {

// Python script that creates REE- and dictionary-encoded tables on the server.
// ree_table     : 6 rows, Sym = ["a","a","a","b","b","b"]
// dict_table    : 5 rows, Sym = ["x","y","z","x","y"]
// reedict_table : 6 rows, Sym = ["a","a","a","b","b","b"], doubly-encoded RunEndEncoded<Dictionary<...>>
static const char *kEncodingSetupScript = R"xxx(
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

_reedict_src    = new_table([string_col('Sym', ['a', 'a', 'a', 'b', 'b', 'b'])])
_reedict_fields = _JArrayList()
_reedict_fields.add(_make_ree_dict_field('Sym', _JUtf8, 'java.lang.String', 0))
_reedict_schema = _JSchema(_reedict_fields)
reedict_table   = _reedict_src.with_attributes({'BarrageSchema': _reedict_schema})
)xxx";

TEST_CASE("Run-end-encoded table is fetched and decoded correctly", "[encoding]") {
  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();

  thm.RunScript(kEncodingSetupScript);
  auto t = thm.FetchTable("ree_table");

  TableMaker expected;
  expected.AddColumn<std::string>("Sym", {"a", "a", "a", "b", "b", "b"});
  TableComparerForTests::Compare(expected, t);
}

TEST_CASE("Dictionary-encoded table is fetched and decoded correctly", "[encoding]") {
  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();

  thm.RunScript(kEncodingSetupScript);
  auto t = thm.FetchTable("dict_table");

  TableMaker expected;
  expected.AddColumn<std::string>("Sym", {"x", "y", "z", "x", "y"});
  TableComparerForTests::Compare(expected, t);
}

TEST_CASE("Run-end + dictionary encoded table is fetched and decoded correctly", "[encoding]") {
  auto client = TableMakerForTests::CreateClient();
  auto thm = client.GetManager();

  thm.RunScript(kEncodingSetupScript);
  auto t = thm.FetchTable("reedict_table");

  TableMaker expected;
  expected.AddColumn<std::string>("Sym", {"a", "a", "a", "b", "b", "b"});
  TableComparerForTests::Compare(expected, t);
}

}  // namespace deephaven::client::tests
