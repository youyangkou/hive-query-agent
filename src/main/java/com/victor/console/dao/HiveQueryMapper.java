package com.victor.console.dao;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.victor.console.entity.HiveQueryBean;
import org.apache.ibatis.annotations.Select;

import java.util.List;


/**
 * @author Gerry
 * @date 2022-08-12
 */
public interface HiveQueryMapper extends BaseMapper<HiveQueryBean> {

    void addHiveQueryBean(HiveQueryBean hiveQueryBean);

    void updateHiveQueryBean(HiveQueryBean hiveQueryBean);

    void deleteHiveQueryBean(String queryId);

    @Select("select * from hive_query_bean where query_id=#{query_id}")
    List<HiveQueryBean> get(String query_id);


}
