-- Le lundi de la semaine d'une date, quel que soit le WEEK_START de la session.
-- date_trunc(week, ...) en depend, et une vue le prend de qui la lit (Metabase
-- impose le dimanche).
{% macro lundi(expr) -%}
    dateadd(day, 1 - dayofweekiso({{ expr }}), ({{ expr }})::date)
{%- endmacro %}
