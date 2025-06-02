import pytest
from sqlalchemy import create_engine, text
from datetime import date
import json

@pytest.fixture(scope="session")
def db_engine():
    return create_engine('postgresql://postgres:postgres@localhost:5432/postgres')

def test_1_validate_actors_ddl():
    expected_ddl = """
    CREATE TYPE film_struct AS (
        film VARCHAR(255),
        votes INTEGER,
        rating DECIMAL(3,1),
        filmid UUID
    );

    CREATE TYPE quality_class_enum AS ENUM ('star', 'good', 'average', 'bad');

    CREATE TABLE actors (
        id UUID PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        films film_struct[] NOT NULL,
        quality_class quality_class_enum,
        is_active BOOLEAN NOT NULL
    )
    """
    assert expected_ddl.strip() == "YOUR_DDL_HERE".strip()


def test_2_cumulative_table_generation(db_engine):
    with db_engine.connect() as conn:
        test_data = [
            (1, 'Actor1', 2022, json.dumps([
                {"film": "Film1", "votes": 1000, "rating": 8.5, "filmid": "f1", "year": 2022},
                {"film": "Film2", "votes": 1000, "rating": 7.5, "filmid": "f2", "year": 2021}
            ])),
            (2, 'Actor2', 2022, json.dumps([
                {"film": "Film3", "votes": 1000, "rating": 6.5, "filmid": "f3", "year": 2022},
                {"film": "Film4", "votes": 1000, "rating": 5.5, "filmid": "f4", "year": 2021}
            ]))
        ]
        
        for id, name, year, films in test_data:
            conn.execute(
                text("INSERT INTO actors (id, name, created_year, films) VALUES (:id, :name, :year, :films::jsonb)"),
                {"id": id, "name": name, "year": year, "films": films}
            )
        conn.commit()
        
        cumulative_query = """
        WITH yearly_stats AS (
            SELECT 
                a.id,
                film_data->>'year' as year,
                AVG((film_data->>'rating')::float) as avg_rating,
                bool_or((film_data->>'year')::int = EXTRACT(YEAR FROM CURRENT_DATE)) as is_active
            FROM actors a,
                jsonb_array_elements(films) as film_data
            WHERE (film_data->>'year')::int <= :target_year
            GROUP BY a.id, film_data->>'year'
        )
        UPDATE actors a
        SET quality_class = 
            CASE 
                WHEN ys.avg_rating > 8 THEN 'star'
                WHEN ys.avg_rating > 7 THEN 'good'
                WHEN ys.avg_rating > 6 THEN 'average'
                ELSE 'bad'
            END,
            is_active = ys.is_active
        FROM yearly_stats ys
        WHERE a.id = ys.id
        AND ys.year = :target_year
        """
        
        conn.execute(text(cumulative_query), {"target_year": 2022})
        conn.commit()
        
        result = conn.execute(text("SELECT id, quality_class, is_active FROM actors"))
        actor_stats = {row[0]: (row[1], row[2]) for row in result}
        
        assert actor_stats[1][0] == 'star'
        assert actor_stats[1][1] is True
        assert actor_stats[2][0] == 'average'
        assert actor_stats[2][1] is True


def test_3_validate_scd_ddl():
    expected_ddl = """
    CREATE TABLE actors_history_scd (
        actor_id BIGINT REFERENCES actors(id),
        quality_class VARCHAR(10) CHECK (quality_class IN ('star', 'good', 'average', 'bad')),
        is_active BOOLEAN NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE,
        is_current BOOLEAN NOT NULL,
        CONSTRAINT valid_dates CHECK (end_date IS NULL OR end_date >= start_date)
    )
    """
    assert expected_ddl.strip() == "YOUR_SCD_DDL_HERE".strip()

def test_4_scd_backfill(db_engine):
    with db_engine.connect() as conn:
        backfill_query = """
        INSERT INTO actors_history_scd (
            actor_id, quality_class, is_active, start_date, end_date, is_current
        )
        SELECT DISTINCT ON (a.id)
            a.id,
            a.quality_class,
            a.is_active,
            make_date(a.created_year, 1, 1) as start_date,
            NULL as end_date,
            TRUE as is_current
        FROM actors a
        ORDER BY a.id, a.created_year DESC
        """
        
        conn.execute(text(backfill_query))
        conn.commit()
        
        result = conn.execute(text("SELECT COUNT(*) FROM actors_history_scd"))
        assert result.scalar() == 2

def test_5_scd_incremental_update(db_engine):
    with db_engine.connect() as conn:
        incremental_query = """
        WITH updates AS (
            SELECT 
                a.id as actor_id,
                a.quality_class,
                a.is_active,
                CURRENT_DATE as start_date
            FROM actors a
            JOIN actors_history_scd h
                ON a.id = h.actor_id
                AND h.is_current = TRUE
            WHERE h.quality_class != a.quality_class
                OR h.is_active != a.is_active
        )
        INSERT INTO actors_history_scd (
            actor_id, quality_class, is_active, start_date, end_date, is_current
        )
        SELECT
            u.actor_id,
            u.quality_class,
            u.is_active,
            u.start_date,
            NULL,
            TRUE
        FROM updates u
        """
        
        # Update test data
        conn.execute(text("""
            UPDATE actors 
            SET quality_class = 'good', is_active = false 
            WHERE id = 1
        """))
        
        conn.execute(text(incremental_query))
        conn.commit()
        
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM actors_history_scd 
            WHERE actor_id = 1
        """))
        assert result.scalar() == 2

if __name__ == "__main__":
    pytest.main([__file__])