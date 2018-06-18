INSERT INTO {{tableName}} VALUES(
    {% for v in variables %}
    '{{v}}',
    {% endfor %}
    '{{lastEntry}}'
)
