--<test:3 name:getRoomTypeInfo
    --<test:2 name:getRoomTypeNImages
        WITH available_room_type_ids AS
        (
            --<test:1 name:getRoomTypeId
                SELECT va.room_type_id
                FROM vw_room_type_availability AS va
                LEFT JOIN room_type ON room_type.id = va.room_type_id
                WHERE va.date BETWEEN ':startDate' AND ':endDate' AND va.total_available > :numOfRoom AND room_type.hotel_id = :hotelId
                    AND room_type.max_adults_with_extra_bed >= :numOfAdult AND room_type.max_guests_with_extra_bed >= :numOfGuest
                GROUP BY va.room_type_id
                HAVING COUNT(va.date) = :totalDays
            -->test:1
        ),
        room_type_images AS
        (
            SELECT avr.room_type_id, STRING_AGG(room_type_image.image_url, '<>' order by room_type_image.id) AS image_urls
            FROM available_room_type_ids AS avr
            LEFT JOIN room_type_image ON room_type_image.room_type_id = avr.room_type_id
            GROUP BY avr.room_type_id
        )
    -->test:2 sql:SELECT * FROM room_type_images
    SELECT room_type.id, room_type.name,
        room_type.max_adults_with_extra_bed AS number_of_adult,
        room_type.max_guests_with_extra_bed AS number_of_guest,
        room_type_images.image_urls
    FROM room_type
    LEFT JOIN room_type_images ON room_type_images.room_type_id = room_type.id
    INNER JOIN available_room_type_ids AS avr ON avr.room_type_id = room_type.id
-->test:3