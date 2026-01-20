with books as (
    select *
    from {{ ref('stg_1001mots_app__books') }}
),

support_modules as (
    select *
    from {{ ref('stg_1001mots_app__children_support_modules') }}
    where book_id is not null
),

mecene as (
    select *
    from {{ ref('mecene') }}
)

select
    m.*,
    sm.module_index,
    sm.book_condition,
    sm.date_created as date_support_module_created,
    b.title
from support_modules sm
left join books b
    on sm.book_id = b.book_id
left join mecene m
    on m.child_id = sm.child_id