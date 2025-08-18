CREATE TABLE user_datelist_int (
    user_id BIGINT,
    datelist_int BIT(32),
    date DATE,
    PRIMARY KEY (user_id, date)
)