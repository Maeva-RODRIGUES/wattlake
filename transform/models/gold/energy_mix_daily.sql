-- Daily energy mix in MWh, one row per day in Paris time.
-- Definitions of every column and share: see _gold.yml.

with half_hours as (

    -- Realised values are average powers over 30 minutes, published at :00 and
    -- :30 only (see silver _silver.yml): each of these rows covers 0.5 h.
    select
        local_date,
        consumption_mw,
        nuclear_mw, wind_mw, solar_mw, hydro_mw, bioenergy_mw, gas_mw, coal_mw, fuel_oil_mw,
        hydro_pumped_storage_generation_mw,
        bioenergy_waste_mw,
        pumped_storage_pumping_mw,
        -- Total production: the eight sources. Pumping is a consumption
        -- (negative values) and is never added to production.
        nuclear_mw + wind_mw + solar_mw + hydro_mw + bioenergy_mw + gas_mw + coal_mw + fuel_oil_mw
            as production_mw,
        wind_mw + solar_mw + hydro_mw + bioenergy_mw as renewable_mw,
        -- Strict definition: hydro without pumped-storage turbining, and only
        -- half of waste (SDES conventions). Null when the technology detail is
        -- not published (2012), never 0.
        wind_mw + solar_mw
            + (hydro_mw - hydro_pumped_storage_generation_mw)
            + (bioenergy_mw - 0.5 * bioenergy_waste_mw)
            as renewable_strict_mw
    from {{ ref('eco2mix_national') }}
    where minute(timestamp_utc) in (0, 30)

),

daily as (

    select
        local_date,
        -- Energy (MWh) = average power (MW) * 0.5 h, summed over the day.
        sum(consumption_mw) * 0.5 as consumption_mwh,
        sum(production_mw) * 0.5 as production_mwh,
        sum(nuclear_mw) * 0.5 as nuclear_mwh,
        sum(wind_mw) * 0.5 as wind_mwh,
        sum(solar_mw) * 0.5 as solar_mwh,
        sum(hydro_mw) * 0.5 as hydro_mwh,
        sum(bioenergy_mw) * 0.5 as bioenergy_mwh,
        sum(gas_mw) * 0.5 as gas_mwh,
        sum(coal_mw) * 0.5 as coal_mwh,
        sum(fuel_oil_mw) * 0.5 as fuel_oil_mwh,
        sum(hydro_pumped_storage_generation_mw) * 0.5 as hydro_pumped_storage_generation_mwh,
        sum(bioenergy_waste_mw) * 0.5 as bioenergy_waste_mwh,
        sum(pumped_storage_pumping_mw) * 0.5 as pumped_storage_pumping_mwh,
        sum(renewable_mw) * 0.5 as renewable_mwh,
        -- Strict share: numerator and denominator over the same half hours,
        -- those where the technology detail exists. Sum of only nulls is null.
        sum(renewable_strict_mw) * 0.5 as renewable_strict_mwh,
        sum(case when renewable_strict_mw is not null then production_mw end) * 0.5
            as production_with_detail_mwh,
        count(production_mw) as half_hours_with_data,
        count(renewable_strict_mw) as half_hours_with_detail,
        -- 46 on the spring clock-change day, 50 in autumn, 48 otherwise.
        cast(date_diff(
            'minute',
            with_timezone(cast(local_date as timestamp), 'Europe/Paris'),
            with_timezone(cast(local_date + interval '1' day as timestamp), 'Europe/Paris')
        ) / 30 as integer) as expected_half_hours
    from half_hours
    group by local_date

)

select
    local_date,
    expected_half_hours,
    half_hours_with_data,
    half_hours_with_data = expected_half_hours as is_complete,
    half_hours_with_detail,
    consumption_mwh,
    production_mwh,
    nuclear_mwh,
    wind_mwh,
    solar_mwh,
    hydro_mwh,
    bioenergy_mwh,
    gas_mwh,
    coal_mwh,
    fuel_oil_mwh,
    hydro_pumped_storage_generation_mwh,
    bioenergy_waste_mwh,
    pumped_storage_pumping_mwh,
    renewable_mwh,
    renewable_strict_mwh,
    production_with_detail_mwh,
    renewable_mwh / nullif(production_mwh, 0) as renewable_share,
    renewable_strict_mwh / nullif(production_with_detail_mwh, 0) as renewable_share_strict
from daily
