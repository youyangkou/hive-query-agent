package com.victor.console.service;

import com.victor.console.entity.HiveQueryBean;

/**
 * @author Gerry
 * @date 2022-08-12
 */
public interface HiveQueryService {

    HiveQueryBean add(HiveQueryBean hiveQueryBean);

    HiveQueryBean get(String queryId);

    void delete(String queryId);

    HiveQueryBean update(HiveQueryBean hiveQueryBean);
}
