CREATE TABLE hotel(
    id serial PRIMARY KEY,
    name VARCHAR (50) NOT NULL
);

CREATE TABLE room_type(
    id serial PRIMARY KEY,
    hotel_id integer NOT NULL references hotel ON DELETE CASCADE,
    name VARCHAR (50) NOT NULL
);