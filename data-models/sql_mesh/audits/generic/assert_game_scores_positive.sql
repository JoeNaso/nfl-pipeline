AUDIT (
  name assert_game_scores_positive,
);

SELECT *
FROM @this_model
WHERE total_away_score < 0 OR total_home_score < 0
;
