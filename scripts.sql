create database dw_inep;

use dw_inep;

CREATE TABLE dim_ano_2020 (
    tf_ano BIGINT,
    ano VARCHAR(255)
);

CREATE TABLE dim_ies_2020 (
    tf_ies BIGINT,
    ies VARCHAR(255)
);

CREATE TABLE dim_uf_2020 (
    tf_uf BIGINT,
    uf VARCHAR(255)
);

CREATE TABLE dim_modalidade_2020 (
    tf_modalidade BIGINT,
    modalidade VARCHAR(255)
);

CREATE TABLE dim_municipio_2020 (
    tf_municipio BIGINT,
    municipio VARCHAR(255)
);

CREATE TABLE dim_curso_2020 (
    tf_curso BIGINT,
    curso VARCHAR(255)
);

CREATE TABLE dim_ano_2021 (
    tf_ano BIGINT,
    ano VARCHAR(255)
);

CREATE TABLE dim_ies_2021 (
    tf_ies BIGINT,
    ies VARCHAR(255)
);

CREATE TABLE dim_uf_2021 (
    tf_uf BIGINT,
    uf VARCHAR(255)
);

CREATE TABLE dim_modalidade_2021 (
    tf_modalidade BIGINT,
    modalidade VARCHAR(255)
);

CREATE TABLE dim_municipio_2021 (
    tf_municipio BIGINT,
    municipio VARCHAR(255)
);

CREATE TABLE dim_curso_2021 (
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

drop table fact_matriculas;
