DROP TABLE IF EXISTS autohome_bbs_content;

CREATE TABLE autohome_bbs_content (
  id int(32) NOT NULL AUTO_INCREMENT,
  title varchar(150) DEFAULT NULL,
  content text,
  pub_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  author varchar(50) DEFAULT NULL,
  author_url varchar(100) DEFAULT NULL,
  reg_time varchar(20) DEFAULT NULL,
  addr varchar(30) DEFAULT NULL,
  attent_vehicle varchar(50) DEFAULT NULL,
  from_url varchar(150) DEFAULT NULL,
  floor varchar(20),
  cdate timestamp,
  PRIMARY KEY (id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

