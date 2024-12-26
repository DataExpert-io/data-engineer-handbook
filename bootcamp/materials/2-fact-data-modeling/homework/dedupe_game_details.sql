-- A query to deduplicate game_details from Day 1 so there's no duplicates
with deduped as (
    select *,
           ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ) as ROW_NUM
    from game_details
)
select * from deduped
where ROW_NUM = 1