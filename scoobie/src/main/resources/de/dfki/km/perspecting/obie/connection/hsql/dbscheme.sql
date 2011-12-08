--HSQLDB schema

DROP TABLE IF EXISTS index_literals CASCADE;
DROP TABLE IF EXISTS map_language CASCADE;
DROP TABLE IF EXISTS tmp_map_language CASCADE;
DROP TABLE IF EXISTS index_resources CASCADE;
DROP TABLE IF EXISTS tmp_index_resources CASCADE;
DROP TABLE IF EXISTS relations CASCADE;
DROP TABLE IF EXISTS tmp_relations CASCADE;
DROP TABLE IF EXISTS symbols CASCADE;
DROP TABLE IF EXISTS tmp_symbols CASCADE;
DROP TABLE IF EXISTS type_clusters CASCADE;

-- Tables used for calculating statistics on cardinalities.
DROP TABLE IF EXISTS SUBJECT_CARD_RELATIONS CASCADE;
DROP TABLE IF EXISTS OBJECT_CARD_RELATIONS CASCADE;

-- Table used for calculating Markov chains on object properties.
DROP TABLE IF EXISTS markov_chain CASCADE;

-- Table used for calculating proper name statistics.
DROP TABLE IF EXISTS proper_noun_rating CASCADE;
 
-- Table used for clustering correlating classes
DROP TABLE IF EXISTS type_clusters CASCADE;
 
-- Table used for counting REGEX matches on datatype properties
DROP TABLE IF EXISTS literals_regex_distribution CASCADE;
 
CREATE TABLE index_literals (
  index int IDENTITY PRIMARY KEY,
  literal varchar(256) NOT NULL,
  prefix int NOT NULL
);

CREATE TABLE tmp_index_resources (
    uri varchar(256) 
);


CREATE TABLE index_resources (
  index int IDENTITY PRIMARY KEY,
  uri varchar(256) NOT NULL
);


CREATE TABLE relations (
  subject int NOT NULL,
  predicate int NOT NULL,
  object int NOT NULL
--  FOREIGN KEY (subject) REFERENCES index_resources(index),
--  FOREIGN KEY (predicate) REFERENCES index_resources(index),
--  FOREIGN KEY (object) REFERENCES index_resources(index)
);

CREATE TABLE tmp_relations (
  s varchar(256),
  p varchar(256),
  o varchar(256)
);

CREATE TABLE symbols (
  subject int,
  predicate int NOT NULL,
  object int NOT NULL,
  belief float NOT NULL,
  type int
--  FOREIGN KEY (subject) REFERENCES index_resources(index),
--  FOREIGN KEY (predicate) REFERENCES index_resources(index),
--  FOREIGN KEY (object) REFERENCES index_literals(index)
);


CREATE TABLE tmp_symbols (
  s varchar(256),
  p varchar(256),
  o varchar(256),
  h int
);

CREATE TABLE SUBJECT_CARD_RELATIONS (
  predicate integer, 
  "count" integer, 
  "sum" numeric, 
  ratio numeric
);
	
CREATE TABLE OBJECT_CARD_RELATIONS (
  predicate integer, 
  "count" integer, 
  "sum" numeric, 
  ratio numeric
);

CREATE TABLE markov_chain ( 
  subject integer,
  predicate integer, 
  object integer,
  probability double precision
);

CREATE TABLE proper_noun_rating (
  cluster int, 
  property int, 
  rating real, 
  coverage real, 
  ambiguity real, 
  idf real
);

CREATE TABLE type_clusters (
  type int, 
  cluster int
);

CREATE TABLE literals_regex_distribution(
  regex varchar(100), 
  property int, 
  ratio float
);

CREATE VIEW histogram_literals AS 
  SELECT symbols.object AS literal, count(DISTINCT symbols.subject) AS "count"   
    FROM symbols  
    GROUP BY symbols.object;

CREATE VIEW ambiguity_symbols AS 
  SELECT symbols.predicate AS attribute, avg(histogram_literals."count") AS avg_references 
    FROM symbols, histogram_literals  
    WHERE histogram_literals.literal = symbols.object  
    GROUP BY symbols.predicate;
  
    
CREATE VIEW histogram_types AS 
  SELECT r.object AS type, count(DISTINCT r.subject) AS "count" 
    FROM relations r 
    WHERE (
      r.predicate IN ( 
        SELECT index_resources.index 
        FROM index_resources 
        WHERE index_resources.uri = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
      )
    ) 
    GROUP BY r.object;

CREATE VIEW histogram_symbols AS 
  SELECT predicate, count(DISTINCT object) 
    FROM index_literals, symbols 
    WHERE ( index = symbols.object ) 
    GROUP BY predicate;

