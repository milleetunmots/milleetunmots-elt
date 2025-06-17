with calendar_with_holidays as (
    select 
        dateadd(day, seq4(), '2000-01-01') as date_day
    from table(generator(rowcount=>20000))
),

mots_months as (
    select
        $1 as month,
        $2 as mots_month
    from
        values
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 4),
        (5, 5),
        (6, 6),
        (7, 7),
        (8, 8),
        (9, 9),
        (10, 10),
        (11, 11),
        (12, 12)
)

select
    row_number() over (order by date_day) as id,
    cast(date_day as date) as full_date,
    to_timestamp_tz(cast(date_day as date)) as full_date_datetime,
    weekiso(full_date) as week_year,
    month(full_date) as month,
    yearofweekiso(full_date) || '-' || {{ format_week('full_date') }} as year_week,
    year(full_date) || '-' || {{ format_month('full_date') }} as year_month,
    year(full_date) as year,
    'Q' || quarter(full_date) as quarter,
    year(full_date) || '-Q' || quarter(full_date) as year_quarter,
    concat('Q', {{ mots_quarter('full_date') }}) as mots_quarter,
    concat(
        {{ mots_year_quarter('full_date') }},
        '-Q',
        {{ mots_quarter('full_date') }}
    ) as mots_year_quarter,
    mots_months.mots_month,
    mots_months.mots_month || '-' || lower(
        monthname(
            date_from_parts(
                cast(left(mots_year_quarter, 4) as int),
                mots_months.mots_month,
                1
            )
        )
    ) as mots_month_name,
    cast(left(mots_year_quarter, 4) as int) as mots_year,
    dayofweekiso(full_date) as day_of_week,
    dayofmonth(full_date) as day_of_month,
    dayofyear(full_date) as day_of_year--,
    --is_holiday_fr
from calendar_with_holidays
left join mots_months
    on month(full_date) = mots_months.month
