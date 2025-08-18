 CREATE TABLE users_cumulated (
     user_id BIGINT,
     dates_active DATE[],
     date DATE,
     PRIMARY KEY (user_id, date)
 );