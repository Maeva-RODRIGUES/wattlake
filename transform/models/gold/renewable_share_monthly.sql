-- Monthly share of renewables in the production mix, one row per month in Paris time.
-- Shares are ratios of energy sums, never averages of daily shares: see _gold.yml.

with monthly as (

    select
        date_trunc('month', local_date) as month,
        sum(expected_hours) as expected_hours,
        sum(hours_with_data) as hours_with_data,
        sum(hours_with_detail) as hours_with_detail,
        -- The current month: its last day is not over yet.
        bool_or(is_partial) as is_partial,
        sum(consumption_mwh) as consumption_mwh,
        sum(production_mwh) as production_mwh,
        sum(renewable_mwh) as renewable_mwh,
        sum(wind_mwh) as wind_mwh,
        sum(solar_mwh) as solar_mwh,
        sum(hydro_mwh) as hydro_mwh,
        sum(bioenergy_mwh) as bioenergy_mwh,
        -- Null for the months without technology detail (2012), never 0.
        sum(renewable_strict_mwh) as renewable_strict_mwh,
        sum(production_with_detail_mwh) as production_with_detail_mwh
    from {{ ref('energy_mix_daily') }}
    group by 1

)

select
    month,
    expected_hours,
    hours_with_data,
    hours_with_detail,
    hours_with_data = expected_hours as is_complete,
    is_partial,
    consumption_mwh,
    production_mwh,
    renewable_mwh,
    wind_mwh,
    solar_mwh,
    hydro_mwh,
    bioenergy_mwh,
    renewable_strict_mwh,
    production_with_detail_mwh,
    renewable_mwh / nullif(production_mwh, 0) as renewable_share,
    renewable_strict_mwh / nullif(production_with_detail_mwh, 0) as renewable_share_strict
from monthly
