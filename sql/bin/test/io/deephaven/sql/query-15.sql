SELECT
  count(DISTINCT AuthorId) as author_count,
  count(DISTINCT Id) as id_count
FROM
  books
