create database dw_inep;

use dw_inep;

CREATE TABLE dim_ano (
    tf_ano BIGINT,
    ano VARCHAR(255)
);


CREATE TABLE dim_ies (
	tf_ies BIGINT,
	ies VARCHAR(255)
);

CREATE TABLE dim_uf (
    tf_uf BIGINT,
    uf VARCHAR(255)
);

CREATE TABLE dim_modalidade (
    tf_modalidade BIGINT,
    modalidade VARCHAR(255)
);

CREATE TABLE dim_municipio (
    tf_municipio BIGINT,
    municipio VARCHAR(255)
);

CREATE TABLE dim_curso (
    tf_curso BIGINT,
    curso VARCHAR(255)
);

CREATE TABLE fact_matriculas (
    matriculas INT,
    tf_ano BIGINT,
    tf_modalidade BIGINT,
    tf_municipio BIGINT,
    tf_uf BIGINT,
    tf_ies BIGINT,
    tf_curso BIGINT
);


select count(*) from fact_matriculas