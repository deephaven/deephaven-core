SELECT
  olympics.olympiad,
  olympics.host_city,
  host_country.country_name
FROM
  (
    VALUES
      (2012, 'London'),
      (2016, 'Rio de Janeiro'),
      (2020, 'Tokyo'),
      (2024, 'Paris'),
      (2028, 'Los Angeles')
  ) as olympics(olympiad, host_city)
  INNER JOIN (
    VALUES
      ('London', 'England'),
      ('Rio de Janeiro', 'Brazil'),
      ('Tokyo', 'Japan')
  ) as host_country(host_city, country_name) ON host_country.host_city = olympics.host_city
