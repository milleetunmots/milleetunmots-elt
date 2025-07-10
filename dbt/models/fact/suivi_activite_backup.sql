WITH child_family AS (

SELECT 
cs.id AS family_id, 
MAX(c.id) AS child_id, 
MAX(c.group_id) AS group_id 
FROM children AS c
LEFT JOIN child_supports AS cs ON c.child_support_id = cs.id
GROUP BY 1 
),


all_calls AS (

select 
    id,
    'call 0' AS call_number,
    supporter_id,
    call0_duration AS duration_call,
    call0_status AS call_done,
    call0_review AS call_review,
    CASE WHEN call0_goals_sms IS NOT NULL AND call0_goals_sms != '' THEN 1 ELSE NULL END AS pm_posee,
    CASE WHEN call0_goals IS NOT NULL AND call0_goals != '' THEN 1 ELSE NULL END AS pm_posee_call,
    CASE WHEN call1_previous_goals_follow_up IN ('1_succeed', '2_tried') THEN 1 ELSE NULL END AS pm_posee_callX_status,
    0 AS duo_call0_1_ok,
    0 AS duo_call1_2_ok,
    0 AS duo_call2_3_ok,
    NULL AS duo_call0_OK_1_KO,
    NULL AS duo_call0_OK_1_OK
from child_supports

UNION ALL 

select 
    id,
    'call 1' AS call_number,
    supporter_id,
    call1_duration AS duration_call,
    call1_status AS call_done,
    call1_review AS call_review,
    CASE WHEN call1_goals_sms IS NOT NULL AND call1_goals_sms != '' THEN 1 ELSE NULL END AS pm_posee,
    CASE WHEN call1_goals IS NOT NULL AND call0_goals != '' THEN 1 ELSE NULL END AS pm_posee_call,
    CASE WHEN call2_previous_goals_follow_up IN ('1_succeed', '2_tried') THEN 1 ELSE NULL END AS pm_posee_callX_status,
    CASE WHEN call0_status = 'OK' AND call1_status = 'OK' THEN 1 ELSE 0 END AS duo_call0_1_ok,
    0 AS duo_call1_2_ok,
    0 AS duo_call2_3_ok,
    CASE WHEN call0_status = 'OK' AND call1_status = 'KO' THEN 1 WHEN call0_status = 'OK' THEN 0 ELSE NULL END AS duo_call0_OK_1_KO,
    CASE WHEN call0_status = 'OK' AND call1_status = 'OK' THEN 1 WHEN call0_status = 'OK' THEN 0 ELSE NULL END AS duo_call0_OK_1_OK

from child_supports

UNION ALL 

select 
    id,
    'call 2' AS call_number,
    supporter_id,
    call2_duration AS duration_call,
    call2_status AS call_done,
    call2_review AS call_review,
    CASE WHEN call2_goals_sms IS NOT NULL AND call2_goals_sms != '' THEN 1 ELSE NULL END AS pm_posee,
    CASE WHEN call2_goals IS NOT NULL AND call0_goals != '' THEN 1 ELSE NULL END AS pm_posee_call,
    CASE WHEN call3_previous_goals_follow_up IN ('1_succeed', '2_tried') THEN 1 ELSE NULL END AS pm_posee_callX_status,
    0 AS duo_call0_1_ok,
    CASE WHEN call1_status = 'OK' AND call2_status = 'OK' THEN 1 ELSE 0 END AS duo_call1_2_ok,
    0 AS duo_call2_3_ok,
    NULL AS duo_call0_OK_1_KO, 
    NULL AS duo_call0_OK_1_OK
from child_supports

UNION ALL 

select 
    id,
    'call 3' AS call_number,
    supporter_id,
    call3_duration AS duration_call,
    call3_status AS call_done,
    call3_review AS call_review,
    CASE WHEN call3_goals_sms IS NOT NULL AND call3_goals_sms != '' THEN 1 ELSE NULL END AS pm_posee,
    CASE WHEN call3_goals IS NOT NULL AND call0_goals != '' THEN 1 ELSE NULL END AS pm_posee_call,
    CASE WHEN call4_previous_goals_follow_up IN ('1_succeed', '2_tried') THEN 1 ELSE NULL END AS pm_posee_callX_status,
    0 AS duo_call0_1_ok,
    0 AS duo_call1_2_ok,
    CASE WHEN call2_status = 'OK' AND call3_status = 'OK' THEN 1 ELSE 0 END AS duo_call2_3_ok,
    NULL AS duo_call0_OK_1_KO,
    NULL AS duo_call0_OK_1_OK
from child_supports

)

SELECT 
cf.family_id,
au.name AS supporter_name, 
au.email, 
g.name AS cohort_name,
DATE(g.started_at) AS started_at,
g.is_excluded_from_analytics,
ac.call_number,
ac.duration_call,
ac.call_done,
ac.pm_posee,
ac.pm_posee_call,
CASE WHEN ac.call_review = '0_very_satisfied' THEN 'très satisfaisant' WHEN ac.call_review = '1_rather_satisfied' THEN 'satisfaisant' WHEN ac.call_review = '2_rather_dissatisfied' THEN 'peu satisfaisant'  WHEN ac.call_review = '3_very_dissatisfied' THEN 'très insatisfaisant' ELSE ac.call_review END AS call_review,
pm_posee_callX_status,
ac.duo_call0_1_ok,
ac.duo_call1_2_ok,
ac.duo_call2_3_ok,
ac.duo_call0_OK_1_KO,
ac.duo_call0_OK_1_OK,
au.is_disabled
FROM all_calls AS ac 
LEFT JOIN child_family AS cf ON ac.id = cf.family_id
LEFT JOIN groups AS g ON cf.group_id = g.id 
LEFT JOIN admin_users AS au ON au.id = ac.supporter_id
where au.name is not null and is_excluded_from_analytics = false