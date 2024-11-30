---------------- Fact Data Modeling LAB 1-----------

-- checking for deduplicates in games data 

-- SELECT 
--        player_id,
--        game_id,
--        team_id,
--        COUNT(1)
-- FROM game_details 
-- GROUP BY 1,2,3
-- HAVING COUNT(1)>1


---- Debeuplicate the data 
-- WITH deduped AS (SELECT *, 
--        ROW_NUMBER() OVER( PARTITION BY game_id,team_id,player_id) as row_num
--        FROM game_details
--        )

-- SELECT * FROM deduped
-- WHERE row_num=1

/* there is lot of issue with this data set such as 

1- there are columns that we dont need 
2-there are columns with is missing 
3- area of important in existing columns in term of functionality

to make our data as fact data model our data should able to answer 
WHAT, WHEN, HOW,WHERE WHOM types of question 
*/

-- to answer when we join game_detials with game data 

-- WITH deduped AS (
--     SELECT g.game_date_est, -- this answer our when
--         gd.*, 
--        ROW_NUMBER() OVER( PARTITION BY gd.game_id,team_id,player_id
--        ORDER BY G.game_date_est) as row_num
--        FROM game_details as gd
--        JOIN games g ON 
--        gd.game_id=g.game_id
--        )

-- SELECT * FROM deduped
-- WHERE row_num=1


/*the above query is where heavy it take couple of sec to run
 to make it query more effeactive we use only import columns 
 form both table and try to de-noramlaize this data

 by removing columns like team details since it will not grow as much 
 like games
*/
-- INSERT INTO fct_game_details

-- WITH deduped AS (
--     SELECT g.game_date_est, -- this answer our when
--         g.season,
--         g.home_team_id,        
--         gd.*, 
--        ROW_NUMBER() OVER( PARTITION BY gd.game_id,team_id,player_id
--        ORDER BY G.game_date_est) as row_num
--        FROM game_details as gd       
--        JOIN games g ON 
--        gd.game_id=g.game_id
--        --WHERE g.game_date_est='2016-10-01'
--        )

-- SELECT  
--       game_date_est AS dim_date,
--       season AS dim_season,
--       team_id AS dim_team_id,
--       player_id AS dim_player_id, 
--       player_name AS dim_player_name,
--       team_id=home_team_id AS dim_playing_at_home,      
--       start_position AS dim_start_position,
--       --comment, as we have parrs this commets blew so we can get red of it 
--       COALESCE(POSITION('DNP' IN comment),0) >0
--        AS dim_did_not_play,
--       COALESCE(POSITION('DND' IN comment),0) >0
--        AS dim_did_not_dress,
--        COALESCE(POSITION('NWT' IN comment),0) >0
--        AS dim_not_with_team,  
--     -- changing min column to its proper type  
--     CAST(SPLIT_PART(min, ':', 1) AS REAL) + 
--     CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60 AS m_mint,
--       fgm AS m_fgm, 
--       fga AS m_fga,
--       fg3a AS m_fg3a,
--       ftm AS m_ftm,
--       fta AS m_fta,
--       oreb AS m_oreb,
--       dreb AS m_dreb,
--       reb AS m_reb,
--       ast AS m_ast,
--       stl AS m_stl,
--       blk as m_blk,
--       "TO" AS m_turn_over,
--       pf AS m_pf,
--       pts AS m_pts,
--       plus_minus AS m_plus_minus
      

--  FROM deduped
-- WHERE row_num=1



---------- Creating fct_game_details table 
-- CREATE Table fct_game_details ( 
--     dim_date  DATE,
--     dim_season INTEGER,
--     dim_team_id INTEGER,
--     dim_player_id INTEGER,
--     dim_player_name TEXT,
--     dim_playing_at_home BOOLEAN,
--     dim_start_position TEXT,
--     dim_did_not_play BOOLEAN,
--     dim_did_not_dress BOOLEAN,
--     dim_not_with_team BOOLEAN,  
     
--     m_mint  REAL,
--     m_fgm INTEGER,
--     m_fga INTEGER,
--     m_fg3a INTEGER,
--     m_ftm INTEGER,
--     m_fta INTEGER,
--     m_oreb INTEGER,
--     m_dreb INTEGER,
--     m_reb INTEGER,
--     m_ast INTEGER,
--     m_stl INTEGER,
--     m_blk INTEGER,
--     m_turn_over INTEGER,
--     m_pf INTEGER,
--     m_pts INTEGER,
--     m_plus_minus INTEGER,
--     PRIMARY KEY (dim_date,dim_team_id,dim_player_id)  
-- )


---------JOINING team and fact game details for getting teams info---

-- SELECT t.*, gd.*
-- FROM teams t JOIN  fct_game_details gd
--     ON t.team_id=gd.dim_team_id


---- cal some facts form fact table 

SELECT dim_player_name,
       COUNT(1) as num_game,
       COUNT(CASE WHEN dim_not_with_team THEN 1 END ) as bailed_out ,
       CAST(COUNT(CASE WHEN dim_not_with_team THEN 1 END  ) AS REAL)/COUNT(1) as bailed_out_per
FROM fct_game_details
GROUP BY 1
ORDER BY 3 DESC

--SELECT COUNT(dim_date) FROM fct_game_details
--DELETE FROM fct_game_details WHERE TRUE 