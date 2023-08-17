select * from {{ ref('pbp_2020') }}
union 
select * from {{ ref('pbp_2021') }}
union 
select * from {{ ref('pbp_2022') }}