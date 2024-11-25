INSERT INTO fct_game_details
with deduped AS (
	SELECT
		g.game_date_est,
		g.season,
		g.home_team_id,
		gd.*,
		ROW_NUMBER() OVER(PARTITION BY gd.game_id,team_id,player_id ORDER BY g.game_date_est) as row_num
	FROM game_details gd
		JOIN games g on gd.game_id = g.game_id
)

SELECT 
	game_date_est as dim_game_date,
	season as dim_season,
	team_id as team_id,
	player_id as dim_player_id,
	player_name as dim_player_name,
	start_position as dim_start_position,
	team_id = home_team_id AS dim_is_playing_at_home,
	COALESCE(POSITION('DNP' in comment),0) > 0 as dim_did_not_play,
	COALESCE(POSITION('DND' in comment),0) > 0 as dim_did_not_dress,
	COALESCE(POSITION('NWT' in comment),0) > 0 as dim_not_with_team,
	CAST(SPLIT_PART(min,':',1) AS REAL)
	 + CAST(SPLIT_PART(min,':',2) AS REAL)/60 AS m_minutes,
	fgm as m_fgm,
	fga as m_fga,
	fg3m as m_fg3m,
	fg3a as m_f3a,
	ftm as m_ftm,
	fta as m_fta,
	oreb as m_oreb,
	dreb as m_dreb,
	reb as m_reb,
	ast as m_ast,
	stl as m_stl,
	blk as m_blk,
	"TO" AS m_turnovers,
	pf as m_pf,
	pts as m_pts,
	plus_minus as m_plus_minus
FROM deduped
WHERE row_num = 1