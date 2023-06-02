SELECT
  true as literal_bool,
  'c' as literal_char,
  'foo' as literal_str,
  CAST(1 as TINYINT) as literal_byte,
  CAST(1 as SMALLINT) as literal_small,
  CAST(1 as INTEGER) as literal_int,
  CAST(1 as BIGINT) as literal_long,
  CAST(1 as REAL) as literal_float,
  CAST(1 as DOUBLE) as literal_double
