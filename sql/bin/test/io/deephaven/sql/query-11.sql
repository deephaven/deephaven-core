SELECT
  count(*) as my_count,
  max(AuthorId) as max_author_id
FROM
  books
