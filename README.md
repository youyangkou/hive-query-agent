# hive-query-agent

## INTRODUCTION
This is a proxy service that supports the submission of Hive asynchronous ETL jobs

##### 实现方案
1. 使用HiveServer2实现异步提交，HiveServer2异步执行底层使用thrift实现
2. 使用线程池+阻塞队列的方式实现任务管理和提交
3. 使用元数据表记录整个提交及执行记录


## REFERENCES
1. https://www.docs4dev.com/docs/zh/apache-hive/3.1.1/reference/HiveServer2_Clients.html
2. https://juejin.cn/post/6910568483126411278
3. https://thrift.apache.org/docs/concepts