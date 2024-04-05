select
    competition_id,
    {{ dbt.replace("name", "'-'", "' '") }} as competition_name,
    {{ dbt.replace("type", "'_'", "' '") }} as competition_type,
    {{ dbt.replace("sub_type", "'_'", "' '") }} as competition_sub_type,
    is_major_national_league,
    country_name,
    confederation
from
    {{ source("raw", "competitions") }}