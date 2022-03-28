{% macro cents_to_dollars(amount, decimals = 2 %}
round( {{amount }} / 100, {{decimals}} )
{% endmacro %} 