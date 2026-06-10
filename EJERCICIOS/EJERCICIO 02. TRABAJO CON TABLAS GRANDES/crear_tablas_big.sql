-- Crea la base de Datos
-- similar a create database BIG;

-- Borramos la BD si existe
DROP SCHEMA IF EXISTS big;

-- Creamos la BD big
CREATE SCHEMA big;
USE big;

-- Voy a crear la tabla de provincias 
create table provincias(
   idprovincia integer NOT NULL,
   provincia varchar(50) NOT NULL, 
   PRIMARY KEY (idprovincia)
 );

DROP TABLE IF EXISTS personas;
CREATE TABLE personas 
(
  idpersona integer UNSIGNED NOT NULL AUTO_INCREMENT,
  apellido char(16) DEFAULT NULL,
  nombre char(16) DEFAULT NULL,
  idprovincia integer DEFAULT NULL,
  PRIMARY KEY (idpersona),
  FOREIGN KEY fk_provincia (idprovincia) REFERENCES provincias (idprovincia)
)
 ENGINE=InnoDB DEFAULT CHARSET=utf8;
