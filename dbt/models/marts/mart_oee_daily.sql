{{
    config(
        materialized='incremental',
        unique_key='oee_key',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        cluster_by=['line_id', 'event_date'],
        tags=['gold', 'oee', 'daily'],
        meta={
            'owner': 'data-engineering',
            'description': 'Daily OEE (Overall Equipment Effectiveness) by machine and shift. '
                           'OEE = Availability × Performance × Quality. '
                           'Values between 0.0 and 1.0. World-class OEE is ≥ 0.85.',
        }
    )
}}

/*
    mart_oee_daily.sql
    ------------------
    Calculates OEE components per machine per shift per day.

    OEE = Availability × Performance × Quality
    - Availability: (planned_time - downtime) / planned_time
    - Performance:  (ideal_cycle_time × actual_output) / available_time
    - Quality:      (good_output) / total_output

    Incremental: only processes dates that have arrived since last run.
    Idempotent: merging on oee_key guarantees safe re-runs.
*/

with sensor_events as (

    select *
    from {{ ref('int_sensor_enriched') }}

    {% if is_incremental() %}
    -- Only process records newer than the latest date in the target
    where event_date > (select max(event_date) from {{ this }})
    {% endif %}

),

shift_schedule as (

    select *
    from {{ ref('stg_shift_schedule') }}

),

-- Aggregate sensor readings to machine-shift-day granularity
machine_shift_aggregates as (

    select
        machine_id,
        machine_name,
        machine_type,
        line_id,
        shift_id,
        cast(event_timestamp as date)            as event_date,

        -- Availability components
        count(case when sensor_type = 'DOWNTIME_EVENT' then 1 end)    as downtime_event_count,
        sum(case when sensor_type = 'DOWNTIME_MINUTES' then sensor_value else 0 end) as total_downtime_minutes,

        -- Performance components
        sum(case when sensor_type = 'CYCLE_COUNT' then sensor_value else 0 end)      as total_cycles,
        avg(case when sensor_type = 'CYCLE_TIME_SECONDS' then sensor_value end)      as avg_cycle_time_seconds,

        -- Quality components
        sum(case when sensor_type = 'GOOD_UNITS' then sensor_value else 0 end)       as good_units,
        sum(case when sensor_type = 'DEFECT_COUNT' then sensor_value else 0 end)     as defect_count,
        sum(case when sensor_type = 'TOTAL_OUTPUT' then sensor_value else 0 end)     as total_output,

        -- Anomaly tracking
        sum(case when is_anomaly_flag then 1 else 0 end)              as anomaly_count,
        count(*)                                                        as total_sensor_readings

    from sensor_events
    group by 1, 2, 3, 4, 5, 6

),

oee_calculated as (

    select
        m.*,
        s.planned_minutes_per_shift,
        s.ideal_cycle_time_seconds,

        -- Availability: how much of planned time was the machine actually running
        round(
            safe_divide(
                (s.planned_minutes_per_shift - m.total_downtime_minutes),
                s.planned_minutes_per_shift
            ), 4
        ) as availability_rate,

        -- Performance: how fast was the machine running vs ideal
        round(
            safe_divide(
                (s.ideal_cycle_time_seconds * m.total_cycles),
                ((s.planned_minutes_per_shift - m.total_downtime_minutes) * 60)
            ), 4
        ) as performance_rate,

        -- Quality: what proportion of output was good
        round(
            safe_divide(m.good_units, nullif(m.total_output, 0)),
            4
        ) as quality_rate

    from machine_shift_aggregates  m
    left join shift_schedule        s on m.shift_id = s.shift_id

),

oee_final as (

    select
        -- Surrogate key for idempotent merge
        {{ dbt_utils.generate_surrogate_key(['machine_id', 'shift_id', 'event_date']) }} as oee_key,

        machine_id,
        machine_name,
        machine_type,
        line_id,
        shift_id,
        event_date,
        planned_minutes_per_shift,

        -- OEE components (clamped to [0, 1] — sensor noise can produce values > 1)
        least(greatest(coalesce(availability_rate, 0), 0), 1) as availability_rate,
        least(greatest(coalesce(performance_rate,  0), 0), 1) as performance_rate,
        least(greatest(coalesce(quality_rate,      0), 0), 1) as quality_rate,

        -- OEE = A × P × Q
        round(
            least(greatest(coalesce(availability_rate, 0), 0), 1) *
            least(greatest(coalesce(performance_rate,  0), 0), 1) *
            least(greatest(coalesce(quality_rate,      0), 0), 1),
            4
        ) as oee_score,

        -- OEE benchmarking tier (industry standard classifications)
        case
            when least(greatest(coalesce(availability_rate, 0), 0), 1) *
                 least(greatest(coalesce(performance_rate,  0), 0), 1) *
                 least(greatest(coalesce(quality_rate,      0), 0), 1) >= 0.85 then 'WORLD_CLASS'
            when least(greatest(coalesce(availability_rate, 0), 0), 1) *
                 least(greatest(coalesce(performance_rate,  0), 0), 1) *
                 least(greatest(coalesce(quality_rate,      0), 0), 1) >= 0.60 then 'ACCEPTABLE'
            else 'NEEDS_IMPROVEMENT'
        end as oee_tier,

        -- Operational counts
        total_cycles,
        avg_cycle_time_seconds,
        good_units,
        defect_count,
        total_output,
        total_downtime_minutes,
        anomaly_count,
        total_sensor_readings,

        -- Audit
        current_timestamp()                    as _dbt_loaded_at,
        '{{ invocation_id }}'                  as _dbt_invocation_id

    from oee_calculated

)

select * from oee_final
