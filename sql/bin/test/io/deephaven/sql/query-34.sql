SELECT
  *
FROM
  books
  INNER JOIN authors ON AuthorId IS NULL AND authors.Id IS NULL
