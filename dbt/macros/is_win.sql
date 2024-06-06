--  1  = win
--  0  = draw
-- -1  = loss
{% macro is_win(own_goals, opponent_goals) %}
    CASE
        WHEN {{ own_goals }} > {{ opponent_goals }} THEN 1
        WHEN {{ own_goals }} = {{ opponent_goals }} THEN 0
        WHEN {{ own_goals }} < {{ opponent_goals }} THEN -1  
        ELSE NULL
    END
{% endmacro %}
