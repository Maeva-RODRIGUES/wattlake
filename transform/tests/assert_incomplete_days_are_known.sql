-- Incomplete days must be the known cases only (docs/sources.md):
--   - every autumn clock-change day (last Sunday of October): the source omits
--     the first of the two repeated hours (quirk 8);
--   - 2026-01-01, whose first half hour is missing at the source.
-- Any other incomplete day means data went missing: the test returns it.

select local_date, expected_half_hours, half_hours_with_data
from {{ ref('energy_mix_daily') }}
where not is_complete
    and not (
        -- Last Sunday of October: a Sunday (ISO day 7) from the 25th onwards.
        (month(local_date) = 10 and day_of_week(local_date) = 7 and day(local_date) >= 25)
        or local_date = date '2026-01-01'
    )
