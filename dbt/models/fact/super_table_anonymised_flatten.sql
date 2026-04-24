WITH base AS (
    SELECT * FROM {{ source('fact_anonymised', 'super_table_anonymised') }}
),

shared_cols AS (
    SELECT
        child_id,
        family_id,
        cohort_name,
        group_status,
        supporter_name,
        support_creation_date,
        gender,
        birthdate,
        age_at_registration,
        age_range_at_start_of_cohort,
        age_today_in_months,
        departement,
        parent1_postal_code,
        parent1_city_name,
        parent1_degree,
        source_name,
        source_channel,
        source_department,
        source_or_family_department,
        is_bilingue,
        ind,
        number_of_children,
        number_of_calls,
        tag_list,
        child_tag_list,
        module2_name,
        module3_name,
        module4_name,
        module5_name,
        module6_name,
        registration_delay,
        is_excluded_from_analytics,
        is_restarted_after_disengaged,
        call_number_when_disengaged,
        call_number_when_disengaged_stop_support,
        stop_support_reason,
        end_of_active_status,
        engagement_state_t1,
        engagement_state_t2,
        is_desengage_t1,
        is_desengage_t2,
        is_estime_desengage_t1,
        is_estime_desengage_t1_conserve,
        is_estime_desengage_t2,
        is_estime_desengage_t2_conserve,
        is_call_0_1_status_ok,
        mid_term_rate,
        mid_term_reaction
    FROM base
)

SELECT
    s.*,
    0                                    AS call_session,
    b.call_0_duration                    AS duration,
    b.call_0_status                      AS status,
    b.call0_sendings_benefits_details    AS sendings_benefits_details,
    b.is_call0_goals                     AS is_goals,
    b.is_call0_status                    AS is_status,
    b.nb_of_tries_call0                  AS nb_of_tries,
    b.review_call0                       AS review,
    b.was_engaged_at_call0               AS was_engaged_at,
    b.was_engage_at_call0                AS was_engage_at,
    NULL::varchar                        AS previous_goals_follow_up
FROM base AS b
JOIN shared_cols AS s USING (child_id, family_id)

UNION ALL

SELECT
    s.*,
    1                                    AS call_session,
    b.call1_duration                     AS duration,
    b.call1_status                       AS status,
    b.call1_sendings_benefits_details    AS sendings_benefits_details,
    b.is_call1_goals                     AS is_goals,
    b.is_call1_status                    AS is_status,
    b.nb_of_tries_call1                  AS nb_of_tries,
    b.review_call1                       AS review,
    b.was_engaged_at_call1               AS was_engaged_at,
    b.was_engage_at_call1                AS was_engage_at,
    b.call1_previous_goals_follow_up     AS previous_goals_follow_up
FROM base AS b
JOIN shared_cols AS s USING (child_id, family_id)

UNION ALL

SELECT
    s.*,
    2                                    AS call_session,
    b.call2_duration                     AS duration,
    b.call2_status                       AS status,
    b.call2_sendings_benefits_details    AS sendings_benefits_details,
    b.is_call2_goals                     AS is_goals,
    b.is_call2_status                    AS is_status,
    b.nb_of_tries_call2                  AS nb_of_tries,
    b.review_call2                       AS review,
    b.was_engaged_at_call2               AS was_engaged_at,
    b.was_engage_at_call2                AS was_engage_at,
    b.call2_previous_goals_follow_up     AS previous_goals_follow_up
FROM base AS b
JOIN shared_cols AS s USING (child_id, family_id)

UNION ALL

SELECT
    s.*,
    3                                    AS call_session,
    b.call3_duration                     AS duration,
    b.call3_status                       AS status,
    b.call3_sendings_benefits_details    AS sendings_benefits_details,
    b.is_call3_goals                     AS is_goals,
    b.is_call3_status                    AS is_status,
    b.nb_of_tries_call3                  AS nb_of_tries,
    b.review_call3                       AS review,
    b.was_engaged_at_call3               AS was_engaged_at,
    b.was_engage_at_call3                AS was_engage_at,
    b.call3_previous_goals_follow_up     AS previous_goals_follow_up
FROM base AS b
JOIN shared_cols AS s USING (child_id, family_id)
