SELECT
  count(*) as my_count,
  max(AuthorId) as max_author_id,
  min(Id) as min_id,
  -- SQLTODO(window-functions): FIRST_VALUE / LAST_VALUE require an OVER clause as of Calcite 1.42
  -- FIRST_VALUE(Id) as first_id,
  -- LAST_VALUE(Id) as last_id,
  avg(Id) as avg_id,
  avg(Id) as avg_id
FROM
  books
