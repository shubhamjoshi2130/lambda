drop table if exists lambda_models.model_repository;

create table lambda_models.model_repository (
model_name string,
model_location string,
model_type varchar(2),
sequence int,
active_flag varchar(1),
is_champion varchar(1)
)
partitioned by (app_name string,version int);