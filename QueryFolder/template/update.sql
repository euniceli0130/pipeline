UPDATE {{tableName}} SET
{% for eachKey, eachVal in toBeUpdated[:-1] %}
{{eachKey}} = {{eachVal}},
{% endfor %}
{{toBeUpdated[-1][0]}} = {{toBeUpdated[-1][1]}}
WHERE
{% for eachKey, eachVal in Criterion[:-1] %}
({{eachKey}} = {{eachVal}}) AND
{% endfor %}
({{Criterion[-1][0]}} = '{{Criterion[-1][1]}}')