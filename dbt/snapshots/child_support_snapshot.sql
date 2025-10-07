{% snapshot child_support_snapshot %}


{{
    config(
        unique_key="id",
        strategy="timestamp",
        updated_at="_sdc_batched_at"
    )
}}

select *
from {{ source('mots_app', 'child_supports') }}

{% endsnapshot %}