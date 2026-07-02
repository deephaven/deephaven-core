library(testthat)
library(rdeephaven)

target <- get_dh_target()

# Python script that creates REE- and dictionary-encoded tables on the server.
# ree_table  : 6 rows, Sym = ["a","a","a","b","b","b"]
# dict_table : 5 rows, Sym = ["x","y","z","x","y"]
ENCODING_SETUP_SCRIPT <- '
import jpy
from deephaven import new_table
from deephaven.column import string_col

_JIntCls     = jpy.get_type("org.apache.arrow.vector.types.pojo.ArrowType$Int")
_JUtf8Cls    = jpy.get_type("org.apache.arrow.vector.types.pojo.ArrowType$Utf8")
_JREECls     = jpy.get_type("org.apache.arrow.vector.types.pojo.ArrowType$RunEndEncoded")
_JDictEncCls = jpy.get_type("org.apache.arrow.vector.types.pojo.DictionaryEncoding")
_JField      = jpy.get_type("org.apache.arrow.vector.types.pojo.Field")
_JFieldType  = jpy.get_type("org.apache.arrow.vector.types.pojo.FieldType")
_JSchema     = jpy.get_type("org.apache.arrow.vector.types.pojo.Schema")
_JHashMap    = jpy.get_type("java.util.HashMap")
_JArrayList  = jpy.get_type("java.util.ArrayList")
_JInt32      = _JIntCls(32, True)
_JUtf8       = _JUtf8Cls()
_JREE        = _JREECls.INSTANCE

def _make_ree_field(col_name, val_type, dh_type_str):
    run_ends = _JField.notNullable("run_ends", _JInt32)
    attrs = _JHashMap()
    attrs.put("deephaven:type", dh_type_str)
    val_children = _JArrayList()
    val_f = _JField("values", _JFieldType(True, val_type, None, attrs), val_children)
    children = _JArrayList()
    children.add(run_ends)
    children.add(val_f)
    return _JField(col_name, _JFieldType(True, _JREE, None, None), children)

_ree_src    = new_table([string_col("Sym", ["a", "a", "a", "b", "b", "b"])])
_ree_fields = _JArrayList()
_ree_fields.add(_make_ree_field("Sym", _JUtf8, "java.lang.String"))
_ree_schema = _JSchema(_ree_fields)
ree_table   = _ree_src.with_attributes({"BarrageSchema": _ree_schema})

_dict_src    = new_table([string_col("Sym", ["x", "y", "z", "x", "y"])])
_dict_enc    = _JDictEncCls(0, False, _JInt32)
_dict_fields = _JArrayList()
_dict_fields.add(_JField("Sym", _JFieldType(True, _JUtf8, _dict_enc, None), _JArrayList()))
_dict_schema = _JSchema(_dict_fields)
dict_table   = _dict_src.with_attributes({"BarrageSchema": _dict_schema})
'

test_that("run-end-encoded table is fetched and decoded correctly", {
  client <- Client$new(target = target)
  client$run_script(ENCODING_SETUP_SCRIPT)

  th  <- client$open_table("ree_table")
  df  <- as.data.frame(th)

  expect_equal(nrow(df), 6)
  expect_equal(df$Sym, c("a", "a", "a", "b", "b", "b"))

  client$close()
})

test_that("dictionary-encoded table is fetched and decoded correctly", {
  client <- Client$new(target = target)
  client$run_script(ENCODING_SETUP_SCRIPT)

  th  <- client$open_table("dict_table")
  df  <- as.data.frame(th)

  expect_equal(nrow(df), 5)
  expect_equal(df$Sym, c("x", "y", "z", "x", "y"))

  client$close()
})

rm(target, ENCODING_SETUP_SCRIPT)
