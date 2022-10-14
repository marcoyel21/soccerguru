CREATE TABLE IF NOT EXISTS h2h.model2 (
    id VARCHAR(12), 
    Y numeric(1, 0),
    league_id VARCHAR(12), 
    season_id VARCHAR(12), 
    venue_id VARCHAR(12), 
    referee_id VARCHAR(12), 
    localteam_id VARCHAR(12),
    visitorteam_id VARCHAR(12),
    localteam_position numeric(2, 0), 
    visitorteam_position numeric(2, 0),
    match_day date,
    UNIQUE KEY  (`id`)
);

