USE sbb;
CREATE TABLE SbbData (
  day DATE,
  ride_desc varchar(255),
  comp_id varchar(255),
  c_sname varchar(255),
  c_name varchar(255),
  prod_id varchar(255),
  line_id int,
  line_txt varchar(255),
  c_id varchar(255),
  v_txt varchar(255),
  add_ride boolean,
  trip_canc boolean,
  stop_id int,
  stop_name varchar(255),
  arr_time DATETIME,
  arr_est DATETIME,
  est_stat varchar(255),
  dep_time DATETIME,
  dep_est DATETIME,
  dep_estat varchar(255),
  train_skip boolean
);
