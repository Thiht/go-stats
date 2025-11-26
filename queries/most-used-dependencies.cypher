MATCH (dependency)<-[:DEPENDS_ON]-(dependent)
WHERE dependent.version = dependent.latest
RETURN dependency.name AS dependency, COUNT(dependent) AS dependents
 ORDER BY dependents DESC
LIMIT 50;
