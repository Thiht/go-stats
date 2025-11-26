MATCH (dependency:Module { name: 'github.com/pkg/errors' })
MATCH (dependent:Module)-[:DEPENDS_ON]->(dependency)
WHERE dependent.version = dependent.latest
MATCH (transitiveDependent:Module)-[:DEPENDS_ON]->(dependent)
WHERE transitiveDependent.version = transitiveDependent.latest
RETURN dependent.name AS dependent, COUNT(transitiveDependent) AS transitiveDependents
 ORDER BY transitiveDependents DESC;
