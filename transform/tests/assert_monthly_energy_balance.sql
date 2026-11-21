-- Monthly electricity balance: consumption must equal production plus the
-- physical exchange balance, pumping and batteries. A physical fact,
-- independent of how gold is built: a production source left out, a sign
-- flipped or a column mapped to the wrong field breaks it.
--
-- consumption = production + exchanges (negative = export)
--             + pumping (negative) + battery charging (negative) + discharging
--
-- Measured on silver for 2012-01 to 2026-11 (179 months): median gap 0.0003 %,
-- largest -0.23 % (2026-08). The threshold, 1 % of consumption, leaves a
-- fourfold margin and still catches a missing source (hydro is about 10 % of
-- production) or a flipped exchange sign (about 10 % of consumption).
-- Lost or duplicated rows are not this test's job: they change the hours
-- covered, which assert_incomplete_days_are_known checks.
-- Returns every month whose gap exceeds the threshold.

with flows as (

    -- Not in gold: exchanges and batteries, from silver; energy = power * step.
    select
        date_trunc('month', local_date) as month,
        sum(physical_exchange_balance_mw * step_hours) as exchange_mwh,
        -- Real-time rows only (since 2026-09): null before, i.e. no energy.
        coalesce(sum(battery_charging_mw * step_hours), 0)
            + coalesce(sum(battery_discharging_mw * step_hours), 0) as battery_mwh
    from {{ ref('eco2mix_national') }}
    group by 1

),

pumping as (

    select
        date_trunc('month', local_date) as month,
        sum(pumped_storage_pumping_mwh) as pumping_mwh
    from {{ ref('energy_mix_daily') }}
    group by 1

),

balance as (

    select
        monthly.month,
        monthly.consumption_mwh,
        monthly.production_mwh,
        flows.exchange_mwh,
        pumping.pumping_mwh,
        flows.battery_mwh,
        monthly.consumption_mwh
            - (monthly.production_mwh + flows.exchange_mwh + pumping.pumping_mwh + flows.battery_mwh)
            as gap_mwh
    from {{ ref('renewable_share_monthly') }} as monthly
    inner join flows on monthly.month = flows.month
    inner join pumping on monthly.month = pumping.month

)

select
    month,
    consumption_mwh,
    production_mwh,
    exchange_mwh,
    pumping_mwh,
    battery_mwh,
    gap_mwh,
    100 * gap_mwh / consumption_mwh as gap_pct
from balance
-- A null gap (a missing flow) fails too: the balance cannot be checked.
where gap_mwh is null
    or abs(gap_mwh) > 0.01 * consumption_mwh
