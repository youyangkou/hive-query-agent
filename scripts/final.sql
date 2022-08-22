create table hive_query_bean
(
    query_id      varchar(255)                         not null  comment '查询id'
        primary key,
    query_sql     varchar(255)                         null comment '查询sql',
    project       varchar(255)                         null comment '查询database',
    tmp_table     varchar(255)                         null comment '生成的临时表',
    is_only_query tinyint(1) default 0                 null comment '是否只是简单查询',
    query_state   varchar(255)                         null comment '查询状态',
    log           text                                 null comment '查询日志',
    update_time   datetime   default CURRENT_TIMESTAMP null comment '更新时间'
)
 comment 'bdp查询引擎记录表'