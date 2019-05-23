MATCH (home:Startpunkt) WITH point({latitude: home.latitude, longitude: home.longitude}) AS home
LOAD CSV FROM "file:///stempelstellen.csv" AS line WITH home, line SKIP 1
WITH home, toInteger(line[0]) AS id, line[1] AS name, split(line[2], " ") AS cords
WITH home, id, name, toFloat(substring(cords[0], 1)) AS latitude, toFloat(substring(cords[1], 1)) AS longitude
WHERE distance(home, point({latitude: latitude, longitude: longitude})) < 40000
MERGE (n:Stempelstelle {id: id}) ON CREATE SET n.latitude = latitude, n.longitude = longitude, n.name = name