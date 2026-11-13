-- Silver eCO2mix national: one row per quarter hour, typed and deduplicated,
-- from the history and the real-time stream (docs/adr/012-temps-reel-silver-gold.md).
-- Grain, units and publication steps of every column: see _silver.yml.

with history as (

    select * from {{ source('bronze', 'eco2mix_national_history') }}

),

-- At the spring clock change, local 02:00-02:45 does not exist, yet the source
-- publishes rows for it, with the same date_heure as the real 03:00-03:45 rows
-- (docs/sources.md, quirk 7). A row is kept only if its local time matches its
-- UTC timestamp converted to Paris time, which drops exactly those artefacts.
-- The real-time dataset has none so far; the same rule guards it.
history_quarter_hours as (

    select *
    from history
    where format_datetime(date_heure at time zone 'Europe/Paris', 'HH:mm') = heure

),

-- The real-time table keeps every version of a quarter hour that the producer
-- published (RTE revisions, overlap window): keep the latest one.
realtime_versions as (

    select
        *,
        row_number() over (
            partition by date_heure
            order by _source_updated_at desc, _kafka_offset desc
        ) as version_rank
    from {{ source('bronze', 'eco2mix_national_realtime') }}
    where format_datetime(date_heure at time zone 'Europe/Paris', 'HH:mm') = heure

),

-- The history (consolidated, then definitive data) replaces the real-time data
-- for every quarter hour it covers, once RTE publishes it.
realtime_quarter_hours as (

    select *
    from realtime_versions
    where version_rank = 1
        and date_heure not in (select date_heure from history_quarter_hours)

),

-- Both sources in the same raw shape. Numbers published as text become
-- integers: "ND" (not available) becomes null, any other non-numeric value
-- makes the cast fail on purpose. step_hours is the duration that one row's
-- realised values cover: the history publishes them every 30 minutes (at :00
-- and :30, null at :15 and :45), the real-time dataset every 15 minutes.
unioned as (

    select
        date_heure, date, nature,
        consommation, prevision_j1, prevision_j,
        nucleaire, eolien, solaire, hydraulique, bioenergies, gaz, charbon, fioul,
        cast(null as bigint) as eolien_terrestre,
        cast(null as bigint) as eolien_offshore,
        hydraulique_fil_eau_eclusee, hydraulique_lacs, hydraulique_step_turbinage,
        bioenergies_dechets, bioenergies_biomasse, bioenergies_biogaz,
        gaz_tac,
        cast(nullif(gaz_cogen, 'ND') as bigint) as gaz_cogen,
        gaz_ccg, gaz_autres,
        fioul_tac, fioul_cogen, fioul_autres,
        pompage,
        cast(null as bigint) as stockage_batterie,
        cast(null as bigint) as destockage_batterie,
        ech_physiques, ech_comm_angleterre, ech_comm_espagne, ech_comm_italie, ech_comm_suisse,
        cast(nullif(ech_comm_allemagne_belgique, 'ND') as bigint) as ech_comm_allemagne_belgique,
        taux_co2,
        0.5 as step_hours,
        _source_dataset,
        _ingested_at
    from history_quarter_hours

    union all

    select
        date_heure, date, nature,
        consommation, prevision_j1, prevision_j,
        nucleaire, eolien, solaire, hydraulique, bioenergies, gaz, charbon, fioul,
        eolien_terrestre,
        eolien_offshore,
        cast(nullif(hydraulique_fil_eau_eclusee, 'ND') as bigint) as hydraulique_fil_eau_eclusee,
        hydraulique_lacs, hydraulique_step_turbinage,
        bioenergies_dechets, bioenergies_biomasse, bioenergies_biogaz,
        gaz_tac,
        gaz_cogen,
        gaz_ccg, gaz_autres,
        fioul_tac, fioul_cogen, fioul_autres,
        pompage,
        stockage_batterie,
        destockage_batterie,
        ech_physiques, ech_comm_angleterre, ech_comm_espagne, ech_comm_italie, ech_comm_suisse,
        ech_comm_allemagne_belgique,
        taux_co2,
        0.25 as step_hours,
        _source_dataset,
        _ingested_at
    from realtime_quarter_hours

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
        cast(step_hours as double) as step_hours,

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
        eolien_terrestre as wind_onshore_mw,
        eolien_offshore as wind_offshore_mw,
        hydraulique_fil_eau_eclusee as hydro_run_of_river_mw,
        hydraulique_lacs as hydro_reservoir_mw,
        hydraulique_step_turbinage as hydro_pumped_storage_generation_mw,
        bioenergies_dechets as bioenergy_waste_mw,
        bioenergies_biomasse as bioenergy_biomass_mw,
        bioenergies_biogaz as bioenergy_biogas_mw,
        gaz_tac as gas_combustion_turbine_mw,
        gaz_cogen as gas_cogeneration_mw,
        gaz_ccg as gas_combined_cycle_mw,
        gaz_autres as gas_other_mw,
        fioul_tac as fuel_oil_combustion_turbine_mw,
        fioul_cogen as fuel_oil_cogeneration_mw,
        fioul_autres as fuel_oil_other_mw,

        -- Storage: pumping and battery charging are stored as negative values
        -- (consumptions), battery discharging as positive values
        pompage as pumped_storage_pumping_mw,
        stockage_batterie as battery_charging_mw,
        destockage_batterie as battery_discharging_mw,

        -- Exchanges: negative means France exports, positive means it imports
        ech_physiques as physical_exchange_balance_mw,
        ech_comm_angleterre as commercial_exchange_great_britain_mw,
        ech_comm_espagne as commercial_exchange_spain_mw,
        ech_comm_italie as commercial_exchange_italy_mw,
        ech_comm_suisse as commercial_exchange_switzerland_mw,
        ech_comm_allemagne_belgique as commercial_exchange_germany_belgium_mw,

        -- Emissions
        taux_co2 as co2_intensity_g_per_kwh,

        -- Lineage
        _source_dataset as source_dataset,
        _ingested_at as ingested_at

    from unioned

)

select * from typed
