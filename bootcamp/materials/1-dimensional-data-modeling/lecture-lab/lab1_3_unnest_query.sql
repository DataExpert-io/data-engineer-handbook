SELECT player_name,
       UNNEST(seasons) -- CROSS JOIN UNNEST
       -- / LATERAL VIEW EXPLODE
FROM players
WHERE current_season = 1998
       AND player_name = 'Michael Jordan';
/* This returns two rows: one for 1996 and one for 1997 
 Recall that when we specify current_season = 1998, in the seasons
 array we have info untill 1998 (including 1998) */