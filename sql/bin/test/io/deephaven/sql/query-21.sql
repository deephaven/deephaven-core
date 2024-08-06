SELECT
  FirstName,
  LastName
FROM
  (
    VALUES
      (1, 'Devin', 'Smith'),
      (2, 'Colin', 'Alworth'),
      (3, 'Ryan', 'Caudy')
  ) AS Coders(CoderId, FirstName, LastName)
WHERE
  CoderId = 2
