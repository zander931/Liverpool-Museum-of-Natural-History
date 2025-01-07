DROP TABLE IF EXISTS exhibition;
DROP TABLE IF EXISTS request_interaction;
DROP TABLE IF EXISTS rating_interaction;
DROP TABLE IF EXISTS department;
DROP TABLE IF EXISTS floor;
DROP TABLE IF EXISTS request;
DROP TABLE IF EXISTS rating ;


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
        request_value = 0 OR request_value = 1
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


INSERT INTO department 
    (department_name)
VALUES
    ('Ecology'),
    ('Zoology'),
    ('Paleontology'),
    ('Geology'),
    ('Entomology')
;

INSERT INTO floor
    (floor_name)
VALUES
    ('Vault'),
    ('1'),
    ('2'),
    ('3')
;

INSERT INTO exhibition
    (exhibition_name, 
    exhibition_description, 
    department_id, 
    floor_id, 
    exhibition_start_date,
    public_id)
VALUES
    ('Adaptation', 
    'How insect evolution has kept pace with an industrialised world.',
    5,
    1,
    '2019-07-01',
    'EXH_01'),
    ('Thunder Lizards',
    'How new research is making scientists rethink what dinosaurs really looked like.',
    3,
    2,
    '2023-02-01',
    'EXH_05'),
    ('Our Polluted World',
    'A hard-hitting exploration of humanity''s impact on the environment.',
    1,
    4,
    '2021-05-12',
    'EXH_04'),
    ('Measureless to Man',
    'An immersive 3D experience: delve deep into a previously-inaccessible cave system.',
    4,
    2,
    '2021-08-23',
    'EXH_00'),
    ('The Crenshaw Collection',
    'An exhibition of 18th Century watercolours, mostly focused on South American wildlife.',
    2,
    3,
    '2021-03-03',
    'EXH_02'),
    ('Cetacean Sensations',
    'Whales: from ancient myth to critically endangered.',
    2,
    2,
    '2019-07-01',
    'EXH_03')
;