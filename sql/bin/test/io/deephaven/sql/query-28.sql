SELECT
  *
FROM
  books
  INNER JOIN authors ON AuthorId + 1 = authors.Id
  AND books.Id <> authors.Id
  AND books.Id = 1