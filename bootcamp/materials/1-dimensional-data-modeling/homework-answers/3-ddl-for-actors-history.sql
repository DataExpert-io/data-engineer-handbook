DO $$ 
BEGIN
 IF NOT EXISTS (
   SELECT 1 FROM information_schema.tables 
   WHERE table_name = 'actors_history_scd'
 ) THEN
   CREATE TABLE actors_history_scd (
     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
     actor_id UUID NOT NULL REFERENCES actors(id),
     quality_class quality_class_enum NOT NULL,
     is_active BOOLEAN NOT NULL,
     start_date TIMESTAMP WITH TIME ZONE NOT NULL,
     end_date TIMESTAMP WITH TIME ZONE,
     created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
     CONSTRAINT valid_dates CHECK (start_date <= COALESCE(end_date, 'infinity'::TIMESTAMP))
   );
 END IF;

 IF NOT EXISTS (
   SELECT 1 FROM pg_indexes 
   WHERE tablename = 'actors_history_scd' 
   AND indexname = 'idx_actors_history_dates'
 ) THEN
   CREATE INDEX idx_actors_history_dates ON actors_history_scd (actor_id, start_date, end_date);
 END IF;
END $$;