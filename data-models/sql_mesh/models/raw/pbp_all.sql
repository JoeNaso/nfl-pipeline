MODEL (
    name raw.pbp_all,
    kind FULL,
    grain [compound_key]
);

select * from pbp_2020
union
select * from pbp_2021
union
select * from pbp_2022
;