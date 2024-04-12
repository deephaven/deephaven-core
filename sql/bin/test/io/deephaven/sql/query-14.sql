SELECT
  AuthorId,
  count(*) as mycount,
  sum(Id) as sum_id
FROM
  books
GROUP BY
  AuthorId,
  Id
