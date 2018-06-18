CREATE TABLE IF NOT EXISTS {{tableName}} (
    {% for v in variables %}
    {{v['columnName']}} {{v['typeName']}}
    {% endfor %}
    PRIMARY KEY({{variables[-1]["primaryKey"]}})
)
