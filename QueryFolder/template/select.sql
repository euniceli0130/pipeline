SELECT
{% if selectedItem[0] != '*' %}
{% for item in selectedItem[:-1] %}
{{item}},
{%endfor%}
{{selectedItem[-1]}}
{% else %}
{{selectedItem[0]}}
{% endif %}
FROM {{tableName}}
WHERE
{%for eachKey, eachVal in Criterion[:-1] %}
({{eachKey}} = {{eachVal}}) AND
{% endfor %}
({{Criterion[-1][0]}} = {{Criterion[-1][1]}})