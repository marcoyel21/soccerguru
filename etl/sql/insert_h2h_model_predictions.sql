INSERT IGNORE INTO h2h.model2_predictions (
    id,
    probs,
    league_id, 
    season_id, 
    venue_id, 
    referee_id, 
    localteam_id,
    visitorteam_id,
    localteam_position, 
    visitorteam_position,
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
  %s
)