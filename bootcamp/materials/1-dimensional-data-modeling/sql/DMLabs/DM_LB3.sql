---------- Creating Vertices ------------


-- CREATE TYPE vertex_type 
-- AS ENUM ( 'player','team','game');



-- CREATE TABLE vertices (
--     identifier TEXT,
--     type vertex_type,
--     properties JSON,
--     PRIMARY KEY(identifier,type)
-- )

---------- Creating EDGEs ------------
-- DROP TYPE IF EXISTS edge_type CASCADE; 
-- CREATE TYPE edge_type AS ENUM (
--     'plays_against',
--     'share_team',
--     'plays_in',
--     'plays_on')


-- CREATE TABLE edges (
--     subject_identifier TEXT,
--     subject_type vertex_type,
--     object_identifier TEXT,
--     object_type vertex_type,
--     edge_type edge_type,
--     properties JSON,
--     PRIMARY KEY (
--         subject_identifier,
--         subject_type,
--         object_identifier,
--         object_type,
--         edge_type
--     )

-- )



-- SELECT type,COUNT(1) FROM vertices GROUP BY type ORDER BY 1


-- SELECT *
-- FROM vertices v
--      JOIN edges e ON 
--      e.subject_identifier=v.identifier AND 
--      e.subject_type=v.type
--      WHERE e.vertex_type ='player' :: vertex_type
-- -- GROUP BY 1 
-- -- ORDER BY 2 ASC

--- JOINING Vertices and edges -------------
SELECT * FROM vertices v JOIN edges e ON 
     v.identifier=e.subject_identifier
     AND v.type=e.subject_type
WHERE e.object_type ='player':: vertex_type

--SELECT DISTINCT(object_type) FROM edges