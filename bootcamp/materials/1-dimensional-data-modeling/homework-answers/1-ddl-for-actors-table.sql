DO $$ 
BEGIN
   EXECUTE 'DROP TYPE IF EXISTS quality_class_enum CASCADE';
   EXECUTE 'DROP TYPE IF EXISTS film_struct CASCADE';
   EXECUTE 'CREATE TYPE quality_class_enum AS ENUM (''star'', ''good'', ''average'', ''bad'')';
   EXECUTE 'CREATE TYPE film_struct AS (
       film VARCHAR(255),
       votes INTEGER,
       rating DECIMAL(3,1),
       filmid UUID
   )';
END $$;

DROP FUNCTION IF EXISTS films_avg_rating CASCADE;
DROP TABLE IF EXISTS actors CASCADE;

CREATE OR REPLACE FUNCTION films_avg_rating(films film_struct[])
RETURNS DECIMAL(3,1) AS $$
BEGIN
   RETURN COALESCE((SELECT AVG(f.rating)::DECIMAL(3,1) FROM unnest(films) AS f), 0);
END;
$$ LANGUAGE plpgsql;

CREATE TABLE actors (
   id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
   name VARCHAR(255) NOT NULL,
   films film_struct[] NOT NULL,
   quality_class quality_class_enum NOT NULL,
   is_active BOOLEAN NOT NULL DEFAULT false,
   CONSTRAINT rating_check CHECK (
       quality_class = CASE 
           WHEN films_avg_rating(films) > 8 THEN 'star'::quality_class_enum
           WHEN films_avg_rating(films) > 7 THEN 'good'::quality_class_enum  
           WHEN films_avg_rating(films) > 6 THEN 'average'::quality_class_enum
           ELSE 'bad'::quality_class_enum
       END
   )
);