{% macro fighter_image_url(fighter_expr) %}
    case
        when {{ fighter_expr }} is null or {{ fighter_expr }} = '' then null
        else concat(
            'http://localhost:8888/',
            regexp_replace(
                regexp_replace(lower({{ fighter_expr }}), '[^a-z0-9]+', '_', 'g'),
                '^_+|_+$',
                '',
                'g'
            ),
            '.png'
        )
    end
{% endmacro %}
