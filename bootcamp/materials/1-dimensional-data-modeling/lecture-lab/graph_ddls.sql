CREATE TYPE vertex_type
AS ENUM ('player', 'team', 'game');



CREATE TABLE vertices (
    identifier TEXT,
    type VERTEX_TYPE,
    properties JSON,
    PRIMARY KEY (identifier, type)
);

CREATE TYPE edge_type AS
ENUM (
    'plays_against',
    'shares_team',
    'plays_in',
    'plays_on'
);

CREATE TABLE edges (
    subject_identifier TEXT,
    subject_type VERTEX_TYPE,
    object_identifier TEXT,
    object_type VERTEX_TYPE,
    edge_type EDGE_TYPE,
    properties JSON,
    PRIMARY KEY (
        subject_identifier,
        subject_type,
        object_identifier,
        object_type,
        edge_type
    )
)
