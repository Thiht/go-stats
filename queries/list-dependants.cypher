MATCH (dependency)-[:IS_DEPENDED_ON_BY]->(dependent)
WHERE dependency.name = 'github.com/stretchr/testify'
RETURN dependency.name, dependent.name
