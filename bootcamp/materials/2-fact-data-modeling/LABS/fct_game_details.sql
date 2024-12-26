
INSERT INTO fct_game_details

with deduped as (

    select g.game_date_est,
           g.season,
           g.home_team_id,
           gd.*,
           ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id  order by game_date_est) as ROW_NUM
    from game_details gd
             join games g on gd.game_id = g.game_id

)

select
    game_date_est as dim_date,
    season as dim_season,
    team_id as dim_team_id,
    player_id as dim_player_id,
    player_name as dim_player_name,
    start_position as dim_start_position,
    team_id = home_team_id as dim_is_playing_at_home,
    coalesce(position('DNP' in comment), 0) > 0 AS dim_did_not_play,
    coalesce(position('DND' in comment), 0) > 0 AS dim_did_not_dress,
    coalesce(position('NWT' in comment), 0) > 0 AS dim_not_with_team,
    COALESCE(split_part(min,':', 1)::REAL, 0) + COALESCE(split_part(min,':', 2):: REAL, 0)/60 as m_minutes,
    fgm as m_fgm,
    fga as m_fga,
    fg3m as m_fg3m,
    fg3a as m_fg3m,
    ftm as m_ftm,
    fta as m_fta,
    oreb as m_oreb,
    dreb as m_dreb,
    reb as m_reb,
    ast as m_ast,
    stl as m_stl,
    blk as m_blk,
    "TO" as m_turnover,
    pf as m_pf,
    pts as m_pts,
    plus_minus as m_plus_minus

from deduped
where ROW_NUM = 1


select dim_player_name,
       count(1) as num_games,
       count(case when dim_not_with_team then 1 end) as bailed,
       count(case when dim_not_with_team then 1 end) / count(1):: real as perct
from fct_game_details
group by 1
order by 4 desc

delete from fct_game_details where 1=1

CREATE TABLE fct_game_details (
                                  dim_date DATE,
                                  dim_season INTEGER,
                                  dim_team_id INTEGER,
                                  dim_player_id INTEGER,
                                  dim_player_name TEXT,
                                  dim_start_position TEXT,
                                  dim_is_playin_at_home BOOLEAN,
                                  dim_did_not_play BOOLEAN,
                                  dim_did_not_dress BOOLEAN,
                                  dim_not_with_team BOOLEAN,
                                  m_minutes REAL,
                                  m_fgm INTEGER,
                                  m_fga INTEGER,
                                  m_fg3m INTEGER,
                                  m_fg3a INTEGER,
                                  m_ftm INTEGER,
                                  m_fta INTEGER,
                                  m_oreb INTEGER,
                                  m_dreb INTEGER,
                                  m_reb INTEGER,
                                  m_ast INTEGER,
                                  m_stl INTEGER,
                                  m_blk INTEGER,
                                  m_turnover INTEGER,
                                  m_pf INTEGER,
                                  m_pts INTEGER,
                                  m_plus_minus INTEGER,
                                  PRIMARY KEY (dim_date, dim_team_id, dim_player_id )
)

