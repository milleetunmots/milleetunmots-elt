{% macro compute_age_in_month(registration_date) %}
{# Compute the age in month of the child at the time of registration #}

case 
    when {{ registration_date }} < 3 then 'A/ <3 mois'
    when {{ registration_date }} <= 12 then 'B/ 3-12 mois'
    when {{ registration_date }} <= 18 then 'C/ 12-18 mois'
    when {{ registration_date }} <= 24 then 'D/ 18-24 mois'
    when {{ registration_date }} <= 30 then 'E/ 24-30 mois'
    when {{ registration_date }} > 30 then 'F/ >30 mois'
    else 'G/ NSP' 
end
{% endmacro %}

{% macro accompagnement_annee_n_moins_2(date_started, ended_at_perso, date_created) %}
{# Calcule si un enfant a été accompagné durant année N-2 #}

case 
    when {{ ended_at_perso }} >= date_trunc('year', current_date - interval '2 year') 
        and {{ date_started }} <= date_trunc('year', current_date - interval '2 year') then 1
    when extract(year from {{ date_started }}) = extract(year from current_date - interval '2 year') then 1
    when extract(year from {{ date_created }}) = extract(year from current_date - interval '2 year') 
        and (extract(year from {{ date_started }}) = extract(year from current_date - interval '1 year')) then 1
    when extract(year from {{ date_created }}) = extract(year from current_date - interval '2 year') 
        and child_status = 'waiting' then 1
    else 0 
end 
{% endmacro %}

{% macro accompagnement_annee_n_moins_1(date_started, ended_at_perso, date_created) %}
{# Calcule si un enfant a été accompagné durant année N-1 #}

case 
    when {{ ended_at_perso }} >= date_trunc('year', current_date - interval '1 year') 
        and {{ date_started }} <= date_trunc('year', current_date - interval '1 year') then 1
    when extract(year from {{ date_started }}) = extract(year from current_date - interval '1 year') then 1
    when extract(year from {{ date_created }}) = extract(year from current_date - interval '1 year') 
        and (extract(year from {{ date_started }}) = extract(year from current_date)) then 1
    when extract(year from {{ date_created }}) = extract(year from current_date - interval '1 year') 
        and child_status = 'waiting' then 1
    else 0 
end
{% endmacro %}

{% macro accompagnement_annee_n_moins_1_decompose(date_started, ended_at_perso, date_created) %}
{# Calcule le type d'accompagnement de l'enfant durant année N-1 #}

case 
    when {{ ended_at_perso }} >= date_trunc('year', current_date - interval '1 year') 
        and {{ date_started }} <= date_trunc('year', current_date - interval '1 year') 
        then 'Enfant accompagne 1 janvier'
    when extract(year from {{ date_started }}) = extract(year from current_date - interval '1 year') 
        then 'Enfant ayant commence son accompagnement dans annee'
    when extract(year from {{ date_created }}) = extract(year from current_date - interval '1 year') 
        and (extract(year from {{ date_started }}) = extract(year from current_date)) 
        then 'Enfant inscrits dont accompagnement pas commence'
    when extract(year from {{ date_created }}) = extract(year from current_date - interval '1 year') 
        and child_status = 'waiting' 
        then 'Enfant inscrits dont accompagnement pas commence et pas planifié'
    else null 
end
{% endmacro %}

{% macro accompagnement_annee_n(date_started, ended_at_perso, date_created) %}
{# Calcule si un enfant a été accompagné durant année N #}

case 
    when {{ ended_at_perso }} >= date_trunc('year', current_date) 
        and {{ date_started }} <= date_trunc('year', current_date) then 1
    when extract(year from {{ date_started }}) = extract(year from current_date) then 1
    when extract(year from {{ date_created }}) = extract(year from current_date) 
        and (extract(year from {{ date_started }}) = extract(year from current_date + interval '1 year')) then 1
    when extract(year from {{ date_created }}) = extract(year from current_date) 
        and child_status = 'waiting' then 1
    else 0 
end
{% endmacro %}

{% macro accompagnement_annee_n_decompose(date_started, ended_at_perso, date_created) %}
{# Calcule le type d'accompagnement de l'enfant durant année N #}

case 
    when {{ ended_at_perso }} >= date_trunc('year', current_date) 
        and {{ date_started }} <= date_trunc('year', current_date) 
        then 'Enfant accompagne 1 janvier'
    when extract(year from {{ date_started }}) = extract(year from current_date) 
        then 'Enfant ayant commence son accompagnement dans annee'
    when extract(year from {{ date_created }}) = extract(year from current_date) 
        and (extract(year from {{ date_started }}) = extract(year from current_date + interval '1 year')) 
        then 'Enfant inscrits dont accompagnement pas commence'
    when extract(year from {{ date_created }}) = extract(year from current_date) 
        and child_status = 'waiting' 
        then 'Enfant inscrits dont accompagnement pas commence et pas planifié'
    else null 
end 
{% endmacro %}

{% macro accompagnement_annee_n_1(date_started, ended_at_perso, date_created) %}
{# Calcule si un enfant a été accompagné durant année N+1 #}

case 
    when {{ ended_at_perso }} >= date_trunc('year', current_date + interval '1 year') 
        and {{ date_started }} <= date_trunc('year', current_date + interval '1 year') then 1
    when extract(year from {{ date_started }}) = extract(year from current_date + interval '1 year') then 1
    when extract(year from {{ date_created }}) = extract(year from current_date + interval '1 year') then 1
    else 0 
end 
{% endmacro %}

{% macro accompagnement_annee_n_1_ajuste(date_started, ended_at_perso) %}
{# Calcule le type d'accompagnement de l'enfant durant année N+1 #}
 
case 
        -- cohort with less than 3 months at the end of current year
        when (date_part('year', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('year', {{ date_started }})) * 12 + 
             (date_part('month', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('month', {{ date_started }})) < 3
        then 1
        
        -- cohort with less than 6 months today, will be between 6-11 months at year end
        when (date_part('year', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('year', {{ date_started }})) * 12 + 
             (date_part('month', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('month', {{ date_started }})) between 6 and 11
            and (date_part('year', current_date) - date_part('year', {{ date_started }})) * 12 + 
                (date_part('month', current_date) - date_part('month', {{ date_started }})) < 6 
        then 0.8
        
        -- cohort with less than 6 months today, will be between 3-11 months at year end
        when (date_part('year', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('year', {{ date_started }})) * 12 + 
             (date_part('month', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('month', {{ date_started }})) between 3 and 11
            and (date_part('year', current_date) - date_part('year', {{ date_started }})) * 12 + 
                (date_part('month', current_date) - date_part('month', {{ date_started }})) < 6 
        then 0.6

        -- cohort with more than 6 months today and end date after year end
        when (date_part('year', current_date) - date_part('year', {{ date_started }})) * 12 + 
             (date_part('month', current_date) - date_part('month', {{ date_started }})) >= 6
            and (date_part('year', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
                 date_part('year', {{ ended_at_perso }})) * 12 + 
                (date_part('month', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
                 date_part('month', {{ ended_at_perso }})) <= 0
    then 1    
    else 0 
end
{% endmacro %}

{% macro accompagnement_annee_n_1_ajuste_decompose(date_started, ended_at_perso) %}
{# Calcule le type d'accompagnement de l'enfant durant année N+1 #}

case 
        -- cohort with less than 3 months at the end of current year
        when (date_part('year', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('year', {{ date_started }})) * 12 + 
             (date_part('month', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('month', {{ date_started }})) < 3
        then 1
        
        -- cohort with less than 6 months today, will be between 6-11 months at year end
        when (date_part('year', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('year', {{ date_started }})) * 12 + 
             (date_part('month', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('month', {{ date_started }})) between 6 and 11
            and (date_part('year', current_date) - date_part('year', {{ date_started }})) * 12 + 
                (date_part('month', current_date) - date_part('month', {{ date_started }})) < 6 
        then 0.8
        
        -- cohort with less than 6 months today, will be between 3-11 months at year end
        when (date_part('year', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('year', {{ date_started }})) * 12 + 
             (date_part('month', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('month', {{ date_started }})) between 3 and 11
            and (date_part('year', current_date) - date_part('year', {{ date_started }})) * 12 + 
                (date_part('month', current_date) - date_part('month', {{ date_started }})) < 6 
        then 0.6

        -- cohort with more than 6 months today and end date after year end
        when (date_part('year', current_date) - date_part('year', {{ date_started }})) * 12 + 
             (date_part('month', current_date) - date_part('month', {{ date_started }})) >= 6
            and (date_part('year', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
                 date_part('year', {{ ended_at_perso }})) * 12 + 
                (date_part('month', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
                 date_part('month', {{ ended_at_perso }})) <= 0
        then 1
        
        else 0 
    end
{% endmacro %}







