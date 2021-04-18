  
CREATE DATABASE test;

USE test;

CREATE TABLE test (
    id INT(11) NOT NULL AUTO_INCREMENT,
    string_type VARCHAR(255) COLLATE utf8_unicode_ci DEFAULT NULL,
    int_type INT(11) DEFAULT NULL,
    float_type DOUBLE DEFAULT NULL,
    bool_type TINYINT(1) DEFAULT NULL,
    timestamp_type DATETIME DEFAULT NULL,
    PRIMARY KEY(id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8 COLLATE = utf8_unicode_ci;
