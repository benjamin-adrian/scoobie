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
 
 
CREATE TABLE index_literals (
  index SERIAL PRIMARY KEY,
  literal varchar(256) NOT NULL,
--  prefix varchar(8) NOT NULL
  prefix int NOT NULL
);

CREATE TABLE index_resources (
  index SERIAL PRIMARY KEY,
  uri varchar(256) NOT NULL UNIQUE
);

CREATE TABLE tmp_index_resources (
    uri varchar(256) 
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

CREATE TABLE type_clusters (
  type int,
  cluster int
)