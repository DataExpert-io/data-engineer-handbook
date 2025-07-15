-- Task 1: A query that does state change tracking for `players`
CREATE TYPE player_state AS ENUM ('New', 'Retired', 'Continued Playing', 'Returned from Retirement', 'Stayed Retired');

CREATE TABLE player_state_tracker(
    player_name TEXT,
    first_recorded_season INT,
    last_recorded_season INT,
    player_state player_state,
    years_active INT[],
    record_date INT,
    PRIMARY KEY (player_name, record_date)
);

TRUNCATE TABLE player_state_tracker;

CREATE OR REPLACE FUNCTION load_data_for_years()
RETURNS void AS $$
DECLARE
    year INTEGER;
BEGIN
    FOR year IN 1996..2022 LOOP
        -- Cumulatively insert players data by seasons
		INSERT INTO player_state_tracker (
			WITH yesterday AS (
			    SELECT * FROM player_state_tracker
			    WHERE record_date = (year - 1)
			),
			today AS (
			    SELECT
					*
			    FROM players
			    WHERE current_season = year
			)
			SELECT
			    COALESCE(y.player_name, t.player_name) AS player_name,
			    COALESCE(y.first_recorded_season, t.current_season) AS first_recorded_season,
			    COALESCE(t.current_season, y.last_recorded_season) AS last_recorded_season,
			    CASE
                    -- A player entering the league should be `New`
			        WHEN y.player_name IS NULL THEN 'New'
                    -- A player staying in the league should be `Continued Playing`
			        WHEN y.player_state IN ('New', 'Continued Playing', 'Returned from Retirement') AND t.is_active THEN 'Continued Playing'
                    -- A player leaving the league should be `Retired`
			        WHEN y.player_state IN ('New', 'Continued Playing', 'Returned from Retirement') AND NOT t.is_active THEN 'Retired'
                    -- A player that stays out of the league should be `Stayed Retired`
					WHEN y.player_state IN ('Retired', 'Stayed Retired') AND NOT t.is_active THEN 'Stayed Retired'
                    -- A player that comes out of retirement should be `Returned from Retirement`
					WHEN y.player_state IN ('Retired', 'Stayed Retired') AND t.is_active THEN 'Returned from Retirement'
			        ELSE NULL
			    END::player_state AS player_state,
			    CASE
			        WHEN y.player_name IS NULL AND t.is_active THEN ARRAY[t.current_season]
			        WHEN t.is_active THEN y.years_active || t.current_season
			        ELSE y.years_active
			    END AS years_active,
			    year AS record_date
			FROM today t
			FULL OUTER JOIN yesterday y ON t.player_name = y.player_name
		);
    END LOOP;
END;
$$ LANGUAGE plpgsql;
SELECT load_data_for_years();


-- Task 2a: A query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data
WITH combined AS (
	-- combine ds to get winning team and season data
	SELECT
		gd.game_id AS game_id,
		gd.team_id AS team_id,
		gd.player_name AS player_name,
		gd.pts AS pts,
		CASE WHEN g.home_team_wins = 1
			THEN home_team_id
			ELSE visitor_team_id
		END AS winning_team,
		g.season AS season
	FROM game_details gd
	JOIN games g ON gd.game_id = g.game_id
),
aggregated_ds AS (
	-- aggregate ds to get grouping sets
	SELECT
		COALESCE(c.player_name, 'Overall') AS player,
		c.team_id AS team,
		SUM(COALESCE(c.pts, 0)) AS points,
		c.season AS season,
		c.winning_team AS winning_team,
		COUNT(1) AS count
	FROM combined c
	GROUP BY GROUPING SETS (
		(player_name, team_id),
		(player_name, season),
		(team_id, winning_team)
	)
	ORDER BY COUNT(1) DESC
)
-- Task 2b: Aggregate this dataset along the following dimensions
-- 1. player and team
-- - Answer questions like who scored the most points playing for one team?
SELECT * FROM aggregated_ds
WHERE player <> 'Overall'
ORDER BY points DESC;
-- 2. player and season
-- - Answer questions like who scored the most points in one season?
SELECT * FROM aggregated_ds
WHERE player IS NOT NULL AND season IS NOT NULL
ORDER BY points DESC;
-- 3. team
-- - Answer questions like which team has won the most games?
SELECT * FROM aggregated_ds
WHERE team IS NOT NULL AND winning_team IS NOT NULL
ORDER BY points DESC;

-- Task 3: A query that uses window functions on `game_details` to find out the following things:
-- Task 3a: What is the most games a team has won in a 90 game stretch?
WITH combined AS (
	-- combine ds to get winning team data
	SELECT
		gd.game_id AS game_id,
		gd.team_id AS team_id,
		gd.player_name AS player_name,
		gd.pts AS pts,
		CASE WHEN g.home_team_wins = 1
			THEN home_team_id
			ELSE visitor_team_id
		END AS winning_team,
		g.season AS season
	FROM game_details gd
	JOIN games g ON gd.game_id = g.game_id
),
team_agg AS(
	-- aggregate team data and mark wins
	SELECT
		game_id,
		team_id,
		SUM(COALESCE(pts, 0)) AS game_points,
		CASE WHEN winning_team = team_id
			THEN 1
			ELSE 0
		END AS won_game
	FROM combined
	GROUP BY game_id, team_id, won_game
	ORDER BY game_id
),
roll_window AS (
	-- use window function to calculate rolling 90 game wins
	SELECT
		game_id,
		team_id,
		won_game,
		SUM(won_game) OVER(PARTITION BY team_id ORDER BY game_id ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS rolling_90_wins
	FROM team_agg
	GROUP BY game_id, team_id, won_game
)
-- get max wins per team and order by descending
SELECT
	city,
	MAX(rolling_90_wins) AS max_wins
FROM roll_window rw
JOIN teams t ON rw.team_id = t.team_id
GROUP BY rw.team_id, t.city
ORDER BY max_wins DESC

-- Task 3b: How many games in a row did LeBron James score over 10 points a game?
WITH streak_identifier AS (
	SELECT
		game_id,
		COALESCE(pts, 0) AS clean_pts,
		CASE WHEN COALESCE(pts, 0) > 10
			THEN 1
			ELSE 0
		END AS is_streak
	FROM game_details
	WHERE player_name = 'LeBron James'
),
streak_summation AS (
	SELECT
 		game_id,
		clean_pts,
		is_streak,
		-- split streaks into groups, adding index when streak is broken
		SUM(CASE WHEN is_streak = 1 THEN 0 ELSE 1 END) OVER(ORDER BY game_id) AS grp
	FROM streak_identifier
)
-- get summed streaks and order by descending
SELECT 
	SUM(is_streak) AS streak,
	grp
FROM streak_summation
GROUP BY grp
ORDER BY streak DESC;