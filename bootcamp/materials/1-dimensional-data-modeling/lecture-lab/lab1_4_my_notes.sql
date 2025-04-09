/* STEP1: */
-- SELECT player_name,
--     UNNEST (seasons)::season_stats as season_info
--     /* ::season_stats is a type cast */
-- FROM players
-- where current_season = 2001
--     and player_name = 'Michael Jordan'
/* ------------------------------------------------------------*/
/* STEP2: */
--     WITH unnested AS (
--         SELECT player_name,
--             UNNEST (seasons)::season_stats as seasons_info
--             /* ::season_stats is a type cast */
--         FROM players
--         where current_season = 2001
--             and player_name = 'Michael Jordan'
--     )
-- SELECT player_name,
--     (seasons_info::season_stats).*
-- from unnested
--     /* ---------------------------------------------------------*/
--     /* STEP3: same query as before but querying for all players instead */
WITH unnested AS (
    SELECT player_name,
        UNNEST (seasons)::season_stats as seasons_info
        /* ::season_stats is a type cast */
    FROM players
    where current_season = 2001
)
SELECT player_name,
    (seasons_info::season_stats).*
from unnested
    /* Notice that the players' names are sorted.
     This helps us apply the run-length encoding data compression method. */