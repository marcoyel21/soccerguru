INSERT IGNORE INTO h2h.source_predictions (
id ,
league_id ,
season_id ,
stage_id ,
round_id ,
group_id ,
aggregate_id ,
venue_id ,
referee_id ,
localteam_id ,
visitorteam_id ,
winner_team_id ,
weather_report ,
commentaries ,
attendance ,
pitch ,
details ,
neutral_venue ,
winning_odds_calculated ,
formations ,
scores ,
time_data ,
coaches ,
standings ,
assistants ,
leg ,
colors ,
deleted ,
is_placeholder,
match_day
)
VALUES (
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s,
%s
)