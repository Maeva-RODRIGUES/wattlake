-- Silver eCO2mix national history: one row per quarter hour, typed and deduplicated.
-- Grain, units and publication steps of every column: see _silver.yml.

with source as (

    select * from {{ source('bronze', 'eco2mix_national_history') }}

),

-- At the spring clock change, local 02:00-02:45 does not exist, yet the source
-- publishes rows for it, with the same date_heure as the real 03:00-03:45 rows
-- (docs/sources.md, quirk 7). A row is kept only if its local time matches its
-- UTC timestamp converted to Paris time, which drops exactly those artefacts.
real_quarter_hours as (

    select *
    from source
    where format_datetime(date_heure at time zone 'Europe/Paris', 'HH:mm') = heure

),

typed as (

    select
        date_heure as timestamp_utc,
        cast(date as date) as local_date,
        case nature
            when 'Données définitives' then 'definitive'
            when 'Données consolidées' then 'consolidated'
            when 'Données temps réel' then 'real_time'
        end as data_status,

        -- Consumption and forecasts
        consommation as consumption_mw,
        prevision_j1 as consumption_forecast_day_ahead_mw,
        prevision_j as consumption_forecast_intraday_mw,

        -- Production by source
        nucleaire as nuclear_mw,
        eolien as wind_mw,
        solaire as solar_mw,
        hydraulique as hydro_mw,
        bioenergies as bioenergy_mw,
        gaz as gas_mw,
        charbon as coal_mw,
        fioul as fuel_oil_mw,

        -- Production detail by technology
        hydraulique_fil_eau_eclusee as hydro_run_of_river_mw,
        hydraulique_lacs as hydro_reservoir_mw,
        hydraulique_step_turbinage as hydro_pumped_storage_generation_mw,
        bioenergies_dechets as bioenergy_waste_mw,
        bioenergies_biomasse as bioenergy_biomass_mw,
        bioenergies_biogaz as bioenergy_biogas_mw,
        gaz_tac as gas_combustion_turbine_mw,
        -- Published as text, with "ND" (not available) for missing values.
        -- Any other non-numeric value makes the cast fail on purpose.
        cast(nullif(gaz_cogen, 'ND') as bigint) as gas_cogeneration_mw,
        gaz_ccg as gas_combined_cycle_mw,
        gaz_autres as gas_other_mw,
        fioul_tac as fuel_oil_combustion_turbine_mw,
        fioul_cogen as fuel_oil_cogeneration_mw,
        fioul_autres as fuel_oil_other_mw,

        -- Storage: pumping is stored as negative values (a consumption)
        pompage as pumped_storage_pumping_mw,

        -- Exchanges: negative means France exports, positive means it imports
        ech_physiques as physical_exchange_balance_mw,
        ech_comm_angleterre as commercial_exchange_great_britain_mw,
        ech_comm_espagne as commercial_exchange_spain_mw,
        ech_comm_italie as commercial_exchange_italy_mw,
        ech_comm_suisse as commercial_exchange_switzerland_mw,
        cast(nullif(ech_comm_allemagne_belgique, 'ND') as bigint) as commercial_exchange_germany_belgium_mw,

        -- Emissions
        taux_co2 as co2_intensity_g_per_kwh,

        -- Lineage
        _source_dataset as source_dataset,
        _ingested_at as ingested_at

    from real_quarter_hours

)

select * from typed
