--<test:5 name:getAvailableHotels
    --<test:4 name:getUniqueHotelNRates
        --<test:3 name:getHotelNRates
            --<test:2 name:getHotelsNRoomType
                --<test:1 name:getHotelsByLocation
                    WITH location AS (
                        SELECT ':locationType' ::TEXT AS type
                    ),
                    hotels_by_location AS (
                        SELECT id, code, name, town_id, town_name_en, image_urls, township_id, township_name_en, attractions
                        FROM hotel_summary, location
                        WHERE CASE
                            WHEN type = 'town' THEN town_id = :locationId
                            WHEN type = 'township' THEN township_id = :locationId
                            WHEN type = 'attraction' THEN :locationId = ANY (attractions)
                        END
                    )
                -->test:1 sql:SELECT * FROM hotels_by_location
                ,
                available_hotels_and_room_type AS (
                    SELECT room_type.hotel_id, va.room_type_id, va.total_available , rate_group.id AS rate_group_id
                    FROM vw_room_type_availability AS va
                    INNER JOIN room_type ON room_type.id = va.room_type_id AND room_type.max_adults_with_extra_bed >= :numOfAdult
                        AND room_type.max_guests_with_extra_bed >= :numOfGuest
                    INNER JOIN hotels_by_location ON hotels_by_location.id = room_type.hotel_id
                    INNER JOIN rate_group ON va.room_type_id = rate_group.room_type_id
                    WHERE va.date BETWEEN ':startDate' AND ':endDate' AND va.total_available >= :numOfRoom
                    GROUP BY room_type.hotel_id, va.room_type_id, va.total_available, rate_group.id
                    HAVING COUNT(va.date) = :totalDays
                )
            -->test:2 sql:SELECT * FROM available_hotels_and_room_type
            ,
            with_rate AS (
                SELECT ahr.hotel_id, MAX(rate) AS max_rate, MIN(rate) AS min_rate, SUM(rate) AS total_rate
                FROM available_hotels_and_room_type AS ahr
                LEFT JOIN rate_group_date_rate AS dr ON dr.rate_group_id = ahr.rate_group_id
                WHERE dr.date BETWEEN ':startDate' AND ':endDate'
                GROUP BY ahr.hotel_id, dr.rate
                HAVING COUNT(dr.date) = :totalDays
            )
        -->test:3 sql:SELECT * FROM with_rate
        ,
        unique_results AS (
            SELECT hotel_id, MIN(min_rate) AS min_standard_rate, MAX(max_rate) AS max_standard_rate, SUM(total_rate) AS total_standard_rate
            FROM with_rate
            GROUP BY hotel_id
        )
    -->test:4 sql:SELECT * FROM unique_results
    SELECT hotel_id, min_standard_rate, max_standard_rate, total_standard_rate,
            code, name, town_id, town_name_en, image_urls, township_id, township_name_en, attractions
    FROM unique_results
    LEFT JOIN hotels_by_location ON hotels_by_location.id = unique_results.hotel_id
-->test:5