SELECT
  *
FROM
  books
  INNER JOIN authors ON AuthorId IS NOT DISTINCT FROM Authors.id
