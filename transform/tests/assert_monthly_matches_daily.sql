-- Monthly energies must equal the sums of the daily energies they come from.
-- Returns every month where a sum differs by more than 0.01 MWh.

with daily_sums as (

    select
        date_trunc('month', local_date) as month,
        sum(production_mwh) as production_mwh,
        sum(renewable_mwh) as renewable_mwh,
        sum(renewable_strict_mwh) as renewable_strict_mwh
    from {{ ref('energy_mix_daily') }}
    group by 1

)

select
    monthly.month,
    monthly.production_mwh,
    daily_sums.production_mwh as daily_production_mwh
from {{ ref('renewable_share_monthly') }} as monthly
full outer join daily_sums on monthly.month = daily_sums.month
where monthly.month is null
    or daily_sums.month is null
    or abs(monthly.production_mwh - daily_sums.production_mwh) > 0.01
    or abs(monthly.renewable_mwh - daily_sums.renewable_mwh) > 0.01
    -- Both null before 2013 (unknown strict energy): equal.
    or abs(coalesce(monthly.renewable_strict_mwh, 0) - coalesce(daily_sums.renewable_strict_mwh, 0)) > 0.01
