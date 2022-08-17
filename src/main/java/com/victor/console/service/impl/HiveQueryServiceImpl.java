package com.victor.console.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.victor.console.dao.HiveQueryMapper;
import com.victor.console.entity.HiveQueryBean;
import com.victor.console.service.HiveQueryService;
import org.springframework.stereotype.Service;

import java.util.List;


/**
 * @author Gerry
 * @date 2022-08-12
 */
@Service
public class HiveQueryServiceImpl extends ServiceImpl<HiveQueryMapper, HiveQueryBean> implements HiveQueryService {

    @Override
    public HiveQueryBean add(HiveQueryBean hiveQueryBean) {
        this.baseMapper.addHiveQueryBean(hiveQueryBean);
        return hiveQueryBean;
    }

    @Override
    public HiveQueryBean get(String queryId) {
        List<HiveQueryBean> hiveQueryBeans = this.baseMapper.get(queryId);
        if (hiveQueryBeans.size() > 0) {
            return hiveQueryBeans.get(0);
        } else {
            return null;
        }
    }

    @Override
    public void delete(String queryId) {
        this.baseMapper.deleteHiveQueryBean(queryId);
    }

    @Override
    public HiveQueryBean update(HiveQueryBean hiveQueryBean) {
        this.baseMapper.updateHiveQueryBean(hiveQueryBean);
        return hiveQueryBean;
    }
}


