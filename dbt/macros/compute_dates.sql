{% macro mots_quarter(column_name) %}
{# "Compute mots quarters: normal Q1 is Q2, etc." #}
    mod(quarter({{ column_name }}), 4)
{% endmacro %}


{% macro mots_year_quarter(column_name) %}
{# "Compute mots year_quarters: oct-dec 2018 = 2019_Q1" #}
    case
        when quarter({{ column_name }}) = 4 then year({{ column_name }})
        else year({{ column_name }})
    end
{% endmacro %}


{% macro format_month(column_name) %}
{# "Format month from 1 to '01' for January, etc." #}
    case
        when month({{ column_name }}) < 10 then LPAD(month({{ column_name }}), 2, 0)
        else to_char(month({{ column_name }}))
    end
{% endmacro %}


{% macro format_week(column_name) %}
{# "Format week from 1 to '01' for week 1, etc." #}
    case
        when week({{ column_name }}) < 10 then LPAD(week({{ column_name }}), 2, 0)
        else to_char(week({{ column_name }}))
    end
{% endmacro %}

{% macro compute_month_name(period_start) %}
{# Compute the month name of period #}

case month({{  period_start }})
    when 1 then 'Janvier'
    when 2 then 'Fevrier'
    when 3 then 'Mars'
    when 4 then 'Avril'
    when 5 then 'Mai'
    when 6 then 'Juin'
    when 7 then 'Juillet'
    when 8 then 'Aout'
    when 9 then 'Septembre'
    when 10 then 'Octobre'
    when 11 then 'Novembre'
    else 'Decembre'
end
{% endmacro %}
