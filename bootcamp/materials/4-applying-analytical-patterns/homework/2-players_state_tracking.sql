with year_to_compute as (
	select coalesce(
		(select max(current_year) + 1 from players_state_tracking)
		, (select min(current_season) from players)
	) as year_to_compute
)

, last_year as (
	select
		*
	from
		players_state_tracking
	where
		current_year = (select year_to_compute - 1 from year_to_compute)
)

, this_year as (
	select
		*
	from
		players
	where
		current_season = (select year_to_compute from year_to_compute)
)

, final as (
	select
		coalesce(ly.player_name, ty.player_name)
		, coalesce(ly.first_active_year, ty.current_season) as first_active_year
		, case 
			when ty.is_active then ty.current_season
			else ly.last_active_year
		end as last_active_year
		, case
			when ly.player_name is null then 'New'
			when ly.last_active_year = ly.current_year --was active last year
				and not ty.is_active then 'Retired' --but not this year WARNING : and ty.player_name is null would not work because it is always here in players
			when ly.last_active_year = ly.current_year --was active last year
				and ty.is_active then 'Continued Playing' --this year too
			when ly.last_active_year < ly.current_year --was not active last year
				and not ty.is_active then 'Stayed Retired' --and was not active this year
			when ly.last_active_year < ly.current_year --was not active last year
				and ty.is_active then 'Returned from Retirement' --but was active this year
			else 'Should not happen'
		end as yearly_active_state
		, coalesce(ly.year_list, array[]::integer[])
			|| case
				when ty.is_active then array[ty.current_season]
				else array[]::integer[]
		
			 
			end 	
		as year_list
		, ty.current_season as current_year
		
	from
		this_year as ty
	full outer join
		last_year as ly
		on ty.player_name = ly.player_name -- using player_name because there is no player_id in players
	
)

insert into players_state_tracking
select * from final

