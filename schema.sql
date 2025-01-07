CREATE TABLE department (
    department_id SMALLINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    department_name VARCHAR(100) NOT NULL
);

CREATE TABLE floor (
    floor_id SMALLINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    floor_name VARCHAR(100) NOT NULL
);

CREATE TABLE exhibition (
    exhibition_id SMALLINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    exhibition_name VARCHAR(100) NOT NULL,
    exhibition_description TEXT NOT NULL,
    department_id SMALLINT NOT NULL,
    floor_id SMALLINT NOT NULL,
    exhibition_start_date DATE DEFAULT CURRENT_DATE,
    public_id TEXT UNIQUE NOT NULL,
    FOREIGN KEY (department_id) REFERENCES department(department_id),
    FOREIGN KEY (floor_id) REFERENCES floor(floor_id)
);

CREATE TABLE request (
    request_id SMALLINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    request_value SMALLINT NOT NULL,
    request_description VARCHAR(100) NOT NULL,
    CONSTRAINT zero_or_one CHECK (
        INT(request_value) == 0 OR INT(request_value) == 1)
    )
);

CREATE TABLE rating (
    rating_id SMALLINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    rating_value SMALLINT NOT NULL,
    rating_description VARCHAR(100) NOT NULL
);

CREATE TABLE request_interaction (
    request_interaction_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    exhibition_id SMALLINT NOT NULL,
    request_id SMALLINT NOT NULL,
    event_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (exhibition_id) REFERENCES exhibition(exhibition_id),
    FOREIGN KEY (request_id) REFERENCES request(request_id)
);

CREATE TABLE rating_interaction (
    rating_interaction_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    exhibition_id SMALLINT NOT NULL,
    rating_id SMALLINT NOT NULL,
    event_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (exhibition_id) REFERENCES exhibition(exhibition_id),
    FOREIGN KEY (rating_id) REFERENCES rating(rating_id)
);
