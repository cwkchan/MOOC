These scripts generally accept a parameter --schemas which is a comma seperated list of coursera databases.  If no database is supplied, it is expected that the db.properties will include a table called coursera_index in the format:

CREATE TABLE `coursera_index` (
  `id` varchar(255) DEFAULT NULL,
  `title` varchar(255) DEFAULT NULL,
  `start` date DEFAULT NULL,
  `end` date DEFAULT NULL,
  `length` smallint(6) DEFAULT NULL,
  `link` varchar(255) DEFAULT NULL,
  `pk` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`pk`)
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=latin1;
 
where coursera_index.id is the name of each coursera course schema you are interested in.

Some items may need to be downloaded.
