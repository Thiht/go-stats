MATCH (dependency)-[:IS_DEPENDED_ON_BY]->(dependent)
RETURN dependency.name AS dependency, COUNT(dependent) AS dependents
ORDER BY dependents DESC
LIMIT 50
