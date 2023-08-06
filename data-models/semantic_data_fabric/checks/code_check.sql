SELECT
    DISTINCT c.t_name as "table_name",
    c.c_name as "column name",
    c.classifiers
FROM
    columns c
WHERE
    c.classifiers like '%PII.name%'