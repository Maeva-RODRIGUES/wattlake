-- Daily energy mix in MWh, one row per day in Paris time.
-- Definitions of every column and share: see _gold.yml.
--
-- Built incrementally (docs/adr/017-reconstruction-incrementale.md): a run
-- recomputes only the days that silver changed since the previous run, plus
-- the days still marked partial, and merges them on local_date. A recomputed
-- day sums all its silver rows, as a full rebuild would.
-- "dbt build --full-refresh" rebuilds the whole table.

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='local_date',
    on_schema_change='append_new_columns'
) }}

with

{% if is_incremental() %}
-- The days to recompute. A silver row that changes is merged with the
-- ingestion time of its new bronze row, later than every row already there:
-- the day's latest ingestion time moves forward. So a day is recomputed when
-- silver has a row ingested after the day's last_ingested_at in gold, or when
-- gold does not have the day yet. Days still partial are recomputed too: at
-- midnight, the day before turns complete without any new row.
-- Before last_ingested_at exists (first run against an older table), every
-- day is recomputed, and on_schema_change adds the column.
changed_days as (

    {% if this_has_column('last_ingested_at') -%}
    select distinct silver.local_date
    from {{ ref('eco2mix_national') }} as silver
    left join {{ this }} as gold on gold.local_date = silver.local_date
    where gold.local_date is null
        or silver.ingested_at > gold.last_ingested_at
    union
    select local_date
    from {{ this }}
    where is_partial
    {%- else -%}
    select distinct local_date
    from {{ ref('eco2mix_national') }}
    {%- endif %}

),
{% endif %}

steps as (

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
            as renewable_strict_mw,
        ingested_at
    from {{ ref('eco2mix_national') }}
    {% if is_incremental() -%}
    where local_date in (select local_date from changed_days)
    {%- endif %}

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
        -- Latest ingestion time of the day's silver rows: what the next
        -- incremental run compares silver against.
        max(ingested_at) as last_ingested_at,
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
    -- the latest quarter hour, its totals are not final. "Current" is the
    -- run's logical time (macros/as_of.sql), so a run replayed later rebuilds
    -- the same rows.
    local_date >= cast({{ as_of() }} at time zone 'Europe/Paris' as date) as is_partial,
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
    renewable_strict_mwh / nullif(production_with_detail_mwh, 0) as renewable_share_strict,
    last_ingested_at
from daily
