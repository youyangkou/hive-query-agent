-- auto-generated definition
create table hive_query_bean
(
    query_id      varchar(255)                         not null
        primary key,
    query_sql     varchar(255)                         null,
    project       varchar(255)                         null,
    tmp_table     varchar(255)                         null,
    is_only_query tinyint(1) default 0                 null,
    query_state   varchar(255)                         null,
    log           varchar(255)                         null,
    update_time   datetime   default CURRENT_TIMESTAMP null
)
    collate = utf8mb4_general_ci;