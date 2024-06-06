{% macro get_points(own_goals, opponent_goals) %}
    CASE
        WHEN {{ own_goals }} > {{ opponent_goals }} THEN 3
        WHEN {{ own_goals }} = {{ opponent_goals }} THEN 1
        WHEN {{ own_goals }} < {{ opponent_goals }} THEN 0  
        ELSE NULL
    END
{% endmacro %}
