MATCH (module)
WITH DISTINCT module.name AS distinctName
RETURN COUNT(distinctName) AS count
