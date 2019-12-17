--<test:1 name:getRoomTypeRates
    SELECT rate_group.room_type_id, rate_group.id AS rate_group_id,
        STRING_AGG(dr.date || ':' ||
        CASE
            WHEN rate_group.based_on_plan_id IS NULL THEN dr.rate
            WHEN rate_group.based_on_plan_id IS NOT NULL THEN ROUND(dr.rate+(dr.rate*(rate_group.additional_percentage/100)),2)
        END
        || ',' || 0.0
        , '<>' ORDER BY dr.date) AS date_rates, xbr.rate AS extra_bed_rate
    FROM rate_group
    LEFT JOIN rate_group_date_rate AS dr ON
        CASE
            WHEN rate_group.based_on_plan_id IS NULL THEN dr.rate_group_id = rate_group.id
            WHEN rate_group.based_on_plan_id IS NOT NULL THEN dr.rate_group_id = rate_group.based_on_plan_id
        END
        AND dr.date BETWEEN ':startDate' AND ':endDate'
    LEFT JOIN room_type_extra_bed_rate AS xbr ON
        CASE
            WHEN rate_group.based_on_plan_id IS NULL THEN xbr.rate_group_id = rate_group.id
            WHEN rate_group.based_on_plan_id IS NOT NULL THEN xbr.rate_group_id = rate_group.based_on_plan_id
        END
    WHERE rate_group.room_type_id = :roomTypeId AND (rate_group.guest_type = ':guestType' OR rate_group.guest_type = 'default')
        AND rate_group.is_active = true AND rate_group.id =
            CASE
                WHEN minimum_advance_days IS NOT NULL AND maximum_advance_days IS NOT NULL
                AND minimum_advance_days <= :advanceDays AND maximum_advance_days >= :advanceDays THEN rate_group.id
                WHEN minimum_advance_days IS NULL AND maximum_advance_days IS NULL THEN rate_group.id
            END
    GROUP BY rate_group.id, title, xbr.rate
-->test:1