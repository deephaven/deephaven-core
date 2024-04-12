SELECT
  *
FROM
  books,
  authors
WHERE
  books.AuthorId = authors.Id
