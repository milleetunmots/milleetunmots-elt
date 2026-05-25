-- Granularité : 1 ligne par famille × session d'appel (0, 1, 2, 3)
-- Toutes les familles de super_table sont incluses, qu'elles aient pris RDV ou non.
--
-- Ce modèle permet de calculer :
--   - % de familles ayant pris RDV parmi les familles actives à la session
--   - % de familles ayant pris RDV parmi les appels OK
--   - Impact des RDVs sur le taux d'appels OK
--   - % de no-shows (2 définitions : aircall et statut d'appel)
--   - % d'annulations et de reprogrammations
--
-- ⚠️  LIMITATION : had_call_during_slot et had_real_call_during_slot
-- ne couvrent que les appels passés via Aircall. Les appels hors Aircall
-- (mobile direct, fixe) ne sont pas détectables.
-- Pour une vue au niveau appel individuel, utiliser calls_with_booking.

with aircall as (
    select * from {{ ref('stg_1001mots_app__aircall_calls') }}
),

super_table as (
    select * from {{ ref('super_table') }}
),

-- Dates de session d'appel par famille (récupérées via le groupe du plus jeune enfant)
group_call_dates as (
    select distinct
        st.family_id,
        g.date_call0_start, g.date_call0_end,
        g.date_call1_start, g.date_call1_end,
        g.date_call2_start, g.date_call2_end,
        g.date_call3_start, g.date_call3_end
    from super_table as st
    inner join {{ ref('child') }} as c on c.child_id = st.child_id
    inner join {{ ref('stg_1001mots_app__groups') }} as g on g.group_id = c.group_id
),

-- SMS Calendly : SMS de type TextMessage contenant 'calendly' (insensible à la casse)
-- Imputés à une session d'appel selon date_occurred :
--   session 0 = tout SMS envoyé jusqu'à date_call0_end
--   session N = SMS entre date_call(N-1)_end et date_callN_end
-- Lien : event.related_id = parent_id → famille via parent_enfant
calendly_sms as (
    select
        gcd.family_id,
        e.event_id,
        e.date_occurred,
        e.body,
        case
            when e.date_occurred <= gcd.date_call0_end                                  then 0
            when e.date_occurred >  gcd.date_call0_end and e.date_occurred <= gcd.date_call1_end then 1
            when e.date_occurred >  gcd.date_call1_end and e.date_occurred <= gcd.date_call2_end then 2
            when e.date_occurred >  gcd.date_call2_end and e.date_occurred <= gcd.date_call3_end then 3
        end as call_session
    from {{ ref('stg_1001mots_app__events') }} as e
    inner join {{ ref('parent_enfant') }} as pe on pe.parent_id = e.related_id
    inner join group_call_dates as gcd on gcd.family_id = pe.child_support_id
    where e.type = 'Events::TextMessage'
      and e.body ilike '%calendly%'
),

-- Agrégation par famille × session
calendly_sms_per_session as (
    select
        family_id,
        call_session,
        count(distinct event_id)        as nb_calendly_sms,
        min(date_occurred)              as first_calendly_sms_date,
        max(date_occurred)              as last_calendly_sms_date
    from calendly_sms
    where call_session is not null
    group by 1, 2
),

-- Dépivotage de super_table : 1 ligne par famille × enfant × session
super_table_long_raw as (
    select
        family_id,
        child_id,
        cohort_name,
        supporter_name,
        group_status,
        0                       as call_session,
        call_0_status           as call_status,
        nb_of_tries_call0       as nb_of_tries,
        was_engaged_at_call0    as was_engaged
    from super_table

    union all

    select
        family_id,
        child_id,
        cohort_name,
        supporter_name,
        group_status,
        1,
        call1_status,
        nb_of_tries_call1,
        was_engaged_at_call1
    from super_table

    union all

    select
        family_id,
        child_id,
        cohort_name,
        supporter_name,
        group_status,
        2,
        call2_status,
        nb_of_tries_call2,
        was_engaged_at_call2
    from super_table

    union all

    select
        family_id,
        child_id,
        cohort_name,
        supporter_name,
        group_status,
        3,
        call3_status,
        nb_of_tries_call3,
        was_engaged_at_call3
    from super_table
),

-- Agrégation au niveau famille × session : certaines familles ont plusieurs enfants
-- (ex: jumeaux) qui partagent les mêmes RDV. On ne veut compter les RDV qu'1 fois par famille.
super_table_long as (
    select
        family_id,
        call_session,
        array_agg(distinct child_id) within group (order by child_id) as child_ids,
        count(distinct child_id)                                      as nb_children,
        max(cohort_name)     as cohort_name,
        max(supporter_name)  as supporter_name,
        max(group_status)    as group_status,
        max(call_status)     as call_status,
        max(nb_of_tries)     as nb_of_tries,
        max(was_engaged)     as was_engaged
    from super_table_long_raw
    group by family_id, call_session
),

-- RDVs valides : on exclut les annulations faites avant l'heure du RDV
-- (RDV annulé avant le créneau = jamais présenté, ne compte pas comme tentative)
-- Numérotation par ordre chronologique au sein de chaque famille × session
valid_bookings as (
    select
        *,
        row_number() over (
            partition by family_id, call_session
            order by scheduled_at
        )                                                                   as booking_number
    from {{ ref('stg_1001mots_app__scheduled_calls') }}
    -- Ajout de tous les RDV annulés mais à exclure dan la seconde partie du dashboard metabase
    --where status = 'scheduled'
    --   or status = 'canceled'-- and canceled_at >= scheduled_at)
),

-- Agrégation des RDVs par famille × session
-- avec flag si un appel aircall a eu lieu pendant le créneau booké
bookings as (
    select
        sc.family_id,
        sc.call_session,
        count(distinct sc.scheduled_call_id)                                as nb_bookings,
        count(distinct case when sc.status = 'canceled' then sc.scheduled_call_id end) as nb_canceled,
        max(case when sc.status = 'scheduled' then 1 else 0 end)           as has_active_booking,
        max(case when sc.status = 'canceled' then 1 else 0 end)            as has_canceled_booking,
        min(sc.date_scheduled)                                              as first_booking_date,
        max(sc.date_scheduled)                                              as last_booking_date,
        max(sc.date_canceled)                                               as last_canceled_date,
        -- Date du RDV reprogrammé = date du booking actif quand il y a aussi eu une annulation
        max(case when sc.status = 'scheduled' then sc.date_scheduled end)   as rescheduled_date,
        listagg(distinct sc.event_type_name, ', ')
            within group (order by sc.event_type_name)                     as event_type_names,
        -- Un appel a-t-il eu lieu pendant le créneau ?
        max(case
            when ac.started_at >= sc.scheduled_at
             and ac.started_at <= dateadd(minute, sc.duration_minutes, sc.scheduled_at)
            then 1 else 0
        end)                                                                as had_call_during_slot,
        -- Un vrai appel (duration > 400s) a-t-il eu lieu pendant le créneau ?
        max(case
            when ac.started_at >= dateadd(minute, -10, sc.scheduled_at)
             and ac.started_at <= dateadd(minute, sc.duration_minutes+10, sc.scheduled_at)
             and ac.duration > 300
            then 1 else 0
        end)                                                                as had_real_call_during_slot
    from valid_bookings as sc
    left join aircall as ac
        on  ac.child_support_id = sc.family_id
        and ac.call_session = sc.call_session
    group by 1, 2
)

select
    st.family_id,
    st.child_ids,
    st.nb_children,
    st.cohort_name,
    st.supporter_name,
    st.group_status,
    st.call_session,
    st.call_status,
    {{ clean_call_status('st.call_status') }}                               as call_status_clean,
    st.nb_of_tries,
    st.was_engaged,

    -- Indicateurs de RDV
    coalesce(b.nb_bookings, 0)                                              as nb_bookings,
    coalesce(b.nb_canceled, 0)                                              as nb_canceled,
    coalesce(b.nb_bookings, 0) - coalesce(b.nb_canceled, 0)                as nb_active_bookings,
    coalesce(b.has_active_booking, 0)                                       as has_active_booking,
    coalesce(b.has_canceled_booking, 0)                                     as has_canceled_booking,
    case when coalesce(b.nb_bookings, 0) > 0 then 1 else 0 end             as has_booking,
    case when {{ clean_call_status('st.call_status') }} is not null then 1 else 0 end as has_call_status,

    -- Colonne catégorielle pour pie chart : statut du RDV au niveau session
    case
        when coalesce(b.nb_bookings, 0) = 0          then 'Sans RDV'
        when coalesce(b.has_active_booking, 0) = 1   then 'RDV pris'
        when coalesce(b.has_canceled_booking, 0) = 1 then 'RDV annulé'
    end                                                                     as session_outcome,

    -- RDV reprogrammé = annulé ET nouveau RDV actif
    case
        when coalesce(b.has_canceled_booking, 0) = 1
         and coalesce(b.has_active_booking, 0) = 1
        then 1 else 0
    end                                                                     as is_rescheduled,

    -- Annulé sans reprogrammation
    case
        when coalesce(b.has_canceled_booking, 0) = 1
         and coalesce(b.has_active_booking, 0) = 0
        then 1 else 0
    end                                                                     as is_canceled_only,

    -- Détails du RDV
    b.first_booking_date,
    b.last_booking_date,
    b.last_canceled_date,
    case when coalesce(b.has_canceled_booking, 0) = 1
          and coalesce(b.has_active_booking, 0) = 1
         then b.rescheduled_date
    end                                                                     as rescheduled_date,
    b.event_type_names,
    coalesce(b.had_call_during_slot, 0)                                     as had_call_during_slot,
    coalesce(b.had_real_call_during_slot, 0)                                as had_real_call_during_slot,

    -- No-show côté aircall : RDV pris mais aucun vrai appel pendant le créneau
    case
        when coalesce(b.has_active_booking, 0) = 1
         and coalesce(b.had_real_call_during_slot, 0) = 0
        then 1 else 0
    end                                                                     as is_no_show_aircall,

    -- No-show côté statut d'appel : RDV pris mais appel ni OK ni Incomplet
    case
        when coalesce(b.has_active_booking, 0) = 1
         and {{ clean_call_status('st.call_status') }} not in ('OK', 'Incomplet / Pas de choix de module')
        then 1 else 0
    end                                                                     as is_no_show_call_status,

    -- -----------------------------------------------------------------------
    -- Colonnes Sankey (source → target par niveau)
    -- Utilisation dans Metabase : grouper par sankey_lX_source + sankey_lX_target, COUNT(*)
    -- puis UNION des 3 niveaux pour obtenir le graphe complet
    -- -----------------------------------------------------------------------

    -- Niveau 1 : toutes les sessions → avec ou sans RDV
    'Sessions'                                                              as sankey_l1_source,
    case
        when coalesce(b.nb_bookings, 0) > 0 then 'Avec RDV'
        else 'Sans RDV'
    end                                                                     as sankey_l1_target,

    -- Niveau 2 : avec RDV → que s'est-il passé ? / sans RDV → statut d'appel directement
    case
        when coalesce(b.nb_bookings, 0) > 0 then 'Avec RDV'
        else 'Sans RDV'
    end                                                                     as sankey_l2_source,
    case
        -- Sans RDV : on descend directement au statut d'appel
        when coalesce(b.nb_bookings, 0) = 0
            then {{ clean_call_status('st.call_status') }}
        -- Avec RDV : on passe par l'état du RDV
        when coalesce(b.had_real_call_during_slot, 0) = 1 then 'Appel effectué'
        when coalesce(b.has_canceled_booking, 0) = 1
         and coalesce(b.has_active_booking, 0)  = 1 then 'Reprogrammé'
        when coalesce(b.has_canceled_booking, 0) = 1
         and coalesce(b.has_active_booking, 0)  = 0 then 'Annulé'
        when coalesce(b.has_active_booking, 0)  = 1 then 'No-show'
        else null
    end                                                                     as sankey_l2_target,

    -- Niveau 3 : uniquement pour les sessions avec RDV → statut d'appel
    case
        when coalesce(b.nb_bookings, 0) = 0 then null
        when coalesce(b.has_canceled_booking, 0) = 1
         and coalesce(b.has_active_booking, 0)  = 1 then 'Reprogrammé'
        when coalesce(b.has_canceled_booking, 0) = 1
         and coalesce(b.has_active_booking, 0)  = 0 then 'Annulé'
        when coalesce(b.had_real_call_during_slot, 0) = 1 then 'Appel effectué'
        when coalesce(b.has_active_booking, 0)  = 1 then 'No-show'
        else null
    end                                                                     as sankey_l3_source,
    case
        when coalesce(b.nb_bookings, 0) = 0 then null
        else {{ clean_call_status('st.call_status') }}
    end                                                                     as sankey_l3_target,

    -- -----------------------------------------------------------------------
    -- Colonnes Sankey 2 (source → target par niveau)
    -- Focus : nb de RDVs → appel effectué ou no-show → statut final
    -- Utilisation dans Metabase : même logique que sankey_, UNION des niveaux
    -- Les sessions sans RDV apparaissent au niveau 2 directement (statut d'appel)
    -- -----------------------------------------------------------------------

    -- Niveau 1 : toutes les sessions → avec ou sans RDV
    'Sessions'                                                              as sankey2_l1_source,
    case
        when coalesce(b.nb_bookings, 0) > 0 then 'Avec RDV'
        else 'Sans RDV'
    end                                                                     as sankey2_l1_target,

    -- Niveau 2 : sans RDV → statut d'appel / avec RDV → nombre de RDVs
    case
        when coalesce(b.nb_bookings, 0) > 0 then 'Avec RDV'
        else 'Sans RDV'
    end                                                                     as sankey2_l2_source,
    case
        when coalesce(b.nb_bookings, 0) = 0
            then {{ clean_call_status('st.call_status') }}
        when coalesce(b.nb_bookings, 0) = 1 then '1 RDV'
        when coalesce(b.nb_bookings, 0) = 2 then '2 RDVs'
        else '3+ RDVs'
    end                                                                     as sankey2_l2_target,

    -- Niveau 3 : nb de RDVs → appel effectué ou no-show
    case
        when coalesce(b.nb_bookings, 0) = 0 then null
        when coalesce(b.nb_bookings, 0) = 1 then '1 RDV'
        when coalesce(b.nb_bookings, 0) = 2 then '2 RDVs'
        else '3+ RDVs'
    end                                                                     as sankey2_l3_source,
    case
        when coalesce(b.nb_bookings, 0) = 0 then null
        when coalesce(b.had_real_call_during_slot, 0) = 1 then 'Appel effectué'
        else 'No-show'
    end                                                                     as sankey2_l3_target,

    -- Niveau 4 : appel effectué / no-show → statut final
    case
        when coalesce(b.nb_bookings, 0) = 0 then null
        when coalesce(b.had_real_call_during_slot, 0) = 1 then 'Appel effectué'
        else 'No-show'
    end                                                                     as sankey2_l4_source,
    case
        when coalesce(b.nb_bookings, 0) = 0 then null
        else {{ clean_call_status('st.call_status') }}
    end                                                                     as sankey2_l4_target,

    -- -----------------------------------------------------------------------
    -- Colonnes Sankey 3 : parcours de rebooking (max 4 RDVs)
    -- Lecture : Nème RDV → outcome (Appel effectué / No-show / Annulé)
    --           Si nb_bookings > N → le Nème RDV était forcément annulé (rebooké)
    --           outcome terminal → statut d'appel (étape finale)
    -- Utilisation dans Metabase : UNION ALL des 5 blocs (step1 à step4 + final)
    -- ⚠️ Limitation : had_real_call_during_slot couvre l'ensemble des créneaux,
    --    pas uniquement le dernier. Approximation : le vrai appel a lieu au dernier RDV.
    -- -----------------------------------------------------------------------

    -- Étape 1 : 1er RDV → outcome ou 2ème RDV
    case when coalesce(b.nb_bookings, 0) >= 1 then '1er RDV' end           as sankey3_step1_source,
    case
        when coalesce(b.nb_bookings, 0) = 0   then null
        when coalesce(b.nb_bookings, 0) >= 2  then '2ème RDV'
        when coalesce(b.had_real_call_during_slot, 0) = 1 then 'Appel effectué'
        when coalesce(b.has_active_booking, 0) = 1       then 'No-show'
        else 'Annulé'
    end                                                                     as sankey3_step1_target,

    -- Étape 2 : 2ème RDV → outcome ou 3ème RDV
    case when coalesce(b.nb_bookings, 0) >= 2 then '2ème RDV' end          as sankey3_step2_source,
    case
        when coalesce(b.nb_bookings, 0) < 2   then null
        when coalesce(b.nb_bookings, 0) >= 3  then '3ème RDV'
        when coalesce(b.had_real_call_during_slot, 0) = 1 then 'Appel effectué'
        when coalesce(b.has_active_booking, 0) = 1       then 'No-show'
        else 'Annulé'
    end                                                                     as sankey3_step2_target,

    -- Étape 3 : 3ème RDV → outcome ou 4ème RDV
    case when coalesce(b.nb_bookings, 0) >= 3 then '3ème RDV' end          as sankey3_step3_source,
    case
        when coalesce(b.nb_bookings, 0) < 3   then null
        when coalesce(b.nb_bookings, 0) >= 4  then '4ème RDV'
        when coalesce(b.had_real_call_during_slot, 0) = 1 then 'Appel effectué'
        when coalesce(b.has_active_booking, 0) = 1       then 'No-show'
        else 'Annulé'
    end                                                                     as sankey3_step3_target,

    -- Étape 4 : 4ème RDV → outcome ou 5ème RDV
    case when coalesce(b.nb_bookings, 0) >= 4 then '4ème RDV' end          as sankey3_step4_source,
    case
        when coalesce(b.nb_bookings, 0) < 4   then null
        when coalesce(b.nb_bookings, 0) >= 5  then '5ème RDV'
        when coalesce(b.had_real_call_during_slot, 0) = 1 then 'Appel effectué'
        when coalesce(b.has_active_booking, 0) = 1       then 'No-show'
        else 'Annulé'
    end                                                                     as sankey3_step4_target,

    -- Étape 5 : 5ème RDV → outcome ou 6ème RDV
    case when coalesce(b.nb_bookings, 0) >= 5 then '5ème RDV' end          as sankey3_step5_source,
    case
        when coalesce(b.nb_bookings, 0) < 5   then null
        when coalesce(b.nb_bookings, 0) >= 6  then '6ème RDV'
        when coalesce(b.had_real_call_during_slot, 0) = 1 then 'Appel effectué'
        when coalesce(b.has_active_booking, 0) = 1       then 'No-show'
        else 'Annulé'
    end                                                                     as sankey3_step5_target,

    -- Étape 6 : 6ème RDV → outcome final (pas de 7ème RDV)
    case when coalesce(b.nb_bookings, 0) >= 6 then '6ème RDV' end          as sankey3_step6_source,
    case
        when coalesce(b.nb_bookings, 0) < 6   then null
        when coalesce(b.had_real_call_during_slot, 0) = 1 then 'Appel effectué'
        when coalesce(b.has_active_booking, 0) = 1       then 'No-show'
        else 'Annulé'
    end                                                                     as sankey3_step6_target,

    -- Étape finale : outcome terminal → statut d'appel
    -- (reçoit les flux de tous les niveaux : step1 si nb=1, step2 si nb=2, etc.)
    case
        when coalesce(b.nb_bookings, 0) = 0 then null
        when coalesce(b.had_real_call_during_slot, 0) = 1 then 'Appel effectué'
        when coalesce(b.has_active_booking, 0) = 1       then 'No-show'
        else 'Annulé'
    end                                                                     as sankey3_final_source,
    case
        when coalesce(b.nb_bookings, 0) = 0 then null
        else {{ clean_call_status('st.call_status') }}
    end                                                                     as sankey3_final_target,

    -- Indicateurs SMS Calendly imputés à la session via les dates du groupe
    coalesce(cs.nb_calendly_sms, 0)                                         as nb_calendly_sms,
    case when coalesce(cs.nb_calendly_sms, 0) > 0 then 1 else 0 end        as had_calendly_sms,
    cs.first_calendly_sms_date,
    cs.last_calendly_sms_date

from super_table_long as st
left join bookings as b
    on b.family_id = st.family_id
    and b.call_session = st.call_session
left join calendly_sms_per_session as cs
    on cs.family_id = st.family_id
    and cs.call_session = st.call_session
