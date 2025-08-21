SELECT
	*
FROM
	player_seasons;

-- struct type to hold temporal dimensions
CREATE TYPE season_stats AS (
	season INTEGER,
	gp INTEGER,
	pts REAL,
	reb REAL,
	ast REAL
);

-- cumulative table created from player_seasons dataset
CREATE TABLE players (
	player_name TEXT,
	HEIGHT TEXT,
	college TEXT,
	country TEXT,
	draft_year TEXT,
	draft_round TEXT,
	draft_number TEXT,
	season_stats season_stats[],
	current_season INTEGER,
	PRIMARY KEY (player_name, current_season)
);

-- check first season
SELECT
	MIN(season)
FROM
	player_seasons;

-- incremental data insert into players tables
INSERT INTO
	players
WITH
	yesterday AS (
		SELECT
			*
		FROM
			players
		WHERE
			current_season = 2000
	),
	today AS (
		SELECT
			*
		FROM
			player_seasons
		WHERE
			season = 2001
	)
SELECT
	COALESCE(t.player_name, y.player_name) AS player_name,
	COALESCE(t.height, y.height) AS HEIGHT,
	COALESCE(t.college, y.college) AS college,
	COALESCE(t.country, y.country) AS country,
	COALESCE(t.draft_year, y.draft_year) AS draft_year,
	COALESCE(t.draft_round, y.draft_round) AS draft_round,
	COALESCE(t.draft_number, y.draft_number) AS draft_number,
	CASE
		WHEN y.season_stats IS NULL THEN ARRAY[
			ROW (t.season, t.gp, t.pts, t.reb, t.ast)::season_stats
		]
		WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[
			ROW (t.season, t.gp, t.pts, t.reb, t.ast)::season_stats
		]
		ELSE y.season_stats
	END AS season_stats,
	COALESCE(t.season, y.current_season + 1) AS current_season
FROM
	today t
	FULL OUTER JOIN yesterday y ON t.player_name = y.player_name;

-- check cumulative table output
SELECT
	*
FROM
	players;

-- check michael jordan season_stats as he retired in 1997 and came back in 2001
-- no season stats null data entered for retired seasons
SELECT
	*
FROM
	players
WHERE
	current_season = 2001
	AND player_name = 'Michael Jordan';

-- how data from cumulative table can be restored back like player_seasons table format
WITH
	unnested AS (
		SELECT
			player_name,
			UNNEST(season_stats)::season_stats AS season_stats
		FROM
			players
		WHERE
			current_season = 2001
			AND player_name = 'Michael Jordan'
	)
SELECT
	player_name,
	(season_stats::season_stats).*
FROM
	unnested;

-- drop table players to recreate it for scoring_class changes
DROP TABLE players;

-- add type to calculate player season based on points
CREATE TYPE scoring_class AS ENUM('bad', 'average', 'good', 'star');

-- recreate players cumulative table with scoring_class and years_since_last_season
CREATE TABLE players (
	player_name TEXT,
	HEIGHT TEXT,
	college TEXT,
	country TEXT,
	draft_year TEXT,
	draft_round TEXT,
	draft_number TEXT,
	season_stats season_stats[],
	scoring_class scoring_class,
	years_since_last_season INTEGER,
	current_season INTEGER,
	PRIMARY KEY (player_name, current_season)
);

-- incremental data insert into players tables
INSERT INTO
	players
WITH
	yesterday AS (
		SELECT
			*
		FROM
			players
		WHERE
			current_season = 2000
	),
	today AS (
		SELECT
			*
		FROM
			player_seasons
		WHERE
			season = 2001
	)
SELECT
	COALESCE(t.player_name, y.player_name) AS player_name,
	COALESCE(t.height, y.height) AS HEIGHT,
	COALESCE(t.college, y.college) AS college,
	COALESCE(t.country, y.country) AS country,
	COALESCE(t.draft_year, y.draft_year) AS draft_year,
	COALESCE(t.draft_round, y.draft_round) AS draft_round,
	COALESCE(t.draft_number, y.draft_number) AS draft_number,
	CASE
		WHEN y.season_stats IS NULL THEN ARRAY[
			ROW (t.season, t.gp, t.pts, t.reb, t.ast)::season_stats
		]
		WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[
			ROW (t.season, t.gp, t.pts, t.reb, t.ast)::season_stats
		]
		ELSE y.season_stats
	END AS season_stats,
	CASE
		WHEN t.season IS NOT NULL THEN CASE
			WHEN t.pts > 20 THEN 'star'
			WHEN t.pts > 15 THEN 'good'
			WHEN t.pts > 10 THEN 'average'
			ELSE 'bad'
		END::scoring_class
		ELSE y.scoring_class
	END AS scoring_class,
	CASE
		WHEN t.season IS NOT NULL THEN 0
		ELSE y.years_since_last_season + 1
	END AS years_since_last_season,
	COALESCE(t.season, y.current_season + 1) AS current_season
FROM
	today t
	FULL OUTER JOIN yesterday y ON t.player_name = y.player_name;

-- check cumulative updated table output with new fields
-- also notice the data is always sorted by player_name as we did full outer join during incremental insert
SELECT
	*
FROM
	players
WHERE
	current_season = 2001;

-- check michael jordan new season_stats, scoring_class and years_since_last_season as he retired in 1997 and came back in 2001
-- no season stats null data entered for retired seasons
SELECT
	*
FROM
	players
WHERE
	current_season = 2001
	AND player_name = 'Michael Jordan';

SELECT
	*
FROM
	players
WHERE
	current_season = 2000
	AND player_name = 'Michael Jordan';


-- compare scoring class in first season and current season
SELECT
	player_name,
	season_stats[1] AS first_season,
	season_stats[CARDINALITY(season_stats)] AS latest_season
FROM
	players
WHERE
	current_season = 2001;

SELECT
	player_name,
	(season_stats[1]::season_stats).pts AS first_season,
	(season_stats[CARDINALITY(season_stats)]::season_stats).pts AS latest_season
FROM
	players
WHERE
	current_season = 2001;

-- calculate percent improvement from first season with latest season
SELECT
	player_name,
	(season_stats[CARDINALITY(season_stats)]::season_stats).pts/
	CASE
		WHEN (season_stats[1]::season_stats).pts = 0 THEN 1
		ELSE (season_stats[1]::season_stats).pts
	END
FROM
	players
WHERE
	current_season = 2001;

-- see how the whole output is calculated without using GROUP BY
-- slowest part of this query is ORDER BY DESC part
SELECT
	player_name,
	(season_stats[CARDINALITY(season_stats)]::season_stats).pts/
	CASE
		WHEN (season_stats[1]::season_stats).pts = 0 THEN 1
		ELSE (season_stats[1]::season_stats).pts
	END
FROM
	players
WHERE
	current_season = 2001
ORDER BY 2 DESC;


-- see how the whole output is calculated without using GROUP BY
-- and scoring_class star players
SELECT
	player_name,
	(season_stats[CARDINALITY(season_stats)]::season_stats).pts /
	CASE
		WHEN (season_stats[1]::season_stats).pts = 0 THEN 1
		ELSE (season_stats[1]::season_stats).pts
	END
FROM
	players
WHERE
	current_season = 2001
	AND scoring_class = 'star';
