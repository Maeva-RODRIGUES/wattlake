-- Daily energy mix in MWh, one row per day in Paris time.
-- Definitions of every column and share: see _gold.yml.

with steps as (

    -- Realised values are average powers over the row's time step, step_hours
    -- (see silver _silver.yml): 0.5 h for the history, which publishes them at
    -- :00 and :30 only, 0.25 h for the real-time data. Rows without realised
    -- values (history :15 and :45) carry nulls and add no energy.
    select
        local_date,
        step_hours,
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

),

daily as (

    select
        local_date,
        -- Energy (MWh) = average power (MW) * step duration (h), summed over the day.
        sum(consumption_mw * step_hours) as consumption_mwh,
        sum(production_mw * step_hours) as production_mwh,
        sum(nuclear_mw * step_hours) as nuclear_mwh,
        sum(wind_mw * step_hours) as wind_mwh,
        sum(solar_mw * step_hours) as solar_mwh,
        sum(hydro_mw * step_hours) as hydro_mwh,
        sum(bioenergy_mw * step_hours) as bioenergy_mwh,
        sum(gas_mw * step_hours) as gas_mwh,
        sum(coal_mw * step_hours) as coal_mwh,
        sum(fuel_oil_mw * step_hours) as fuel_oil_mwh,
        sum(hydro_pumped_storage_generation_mw * step_hours) as hydro_pumped_storage_generation_mwh,
        sum(bioenergy_waste_mw * step_hours) as bioenergy_waste_mwh,
        sum(pumped_storage_pumping_mw * step_hours) as pumped_storage_pumping_mwh,
        sum(renewable_mw * step_hours) as renewable_mwh,
        -- Strict share: numerator and denominator over the same steps, those
        -- where the technology detail exists. Sum of only nulls is null.
        sum(renewable_strict_mw * step_hours) as renewable_strict_mwh,
        sum(case when renewable_strict_mw is not null then production_mw * step_hours end)
            as production_with_detail_mwh,
        -- Hours covered by a known production, and by the technology detail.
        coalesce(sum(case when production_mw is not null then step_hours end), 0) as hours_with_data,
        coalesce(sum(case when renewable_strict_mw is not null then step_hours end), 0) as hours_with_detail,
        -- 23 on the spring clock-change day, 25 in autumn, 24 otherwise.
        cast(date_diff(
            'minute',
            with_timezone(cast(local_date as timestamp), 'Europe/Paris'),
            with_timezone(cast(local_date + interval '1' day as timestamp), 'Europe/Paris')
        ) as double) / 60 as expected_hours
    from steps
    group by local_date

)

select
    local_date,
    expected_hours,
    hours_with_data,
    hours_with_data = expected_hours as is_complete,
    -- The current day in Paris time: the real-time data only covers it up to
    -- the latest quarter hour, its totals are not final.
    local_date >= cast(current_timestamp at time zone 'Europe/Paris' as date) as is_partial,
    hours_with_detail,
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
