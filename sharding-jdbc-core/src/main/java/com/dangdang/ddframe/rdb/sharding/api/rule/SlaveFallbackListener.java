package com.dangdang.ddframe.rdb.sharding.api.rule;

import com.dangdang.ddframe.rdb.sharding.executor.event.DMLExecutionEvent;
import com.dangdang.ddframe.rdb.sharding.executor.event.EventExecutionType;
import com.dangdang.ddframe.rdb.sharding.util.EventBusInstance;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.RateLimiter;

import javax.sql.DataSource;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;

/**
 * Created by guoyubo on 2017/7/26.
 */
@Slf4j
public class SlaveFallbackListener {

  private final DataSourceRule dataSourceRule;

  private final ConcurrentHashMap<String, DataSourceLimiter> dataSourceLimiterMap;

  public SlaveFallbackListener(final DataSourceRule dataSourceRule) {
    this.dataSourceRule = dataSourceRule;
    dataSourceLimiterMap = new ConcurrentHashMap<>();
  }

  @Subscribe
  @AllowConcurrentEvents
  public void listen(final DMLExecutionEvent event) {
    if (event.getEventExecutionType() != EventExecutionType.EXECUTE_FAILURE) {
      return;
    }

    if (!dataSourceLimiterMap.containsKey(event.getDataSource())) {
      DataSourceLimiter dataSourceLimiter = new DataSourceLimiter(event.getDataSource());
      dataSourceLimiterMap.put(event.getDataSource(), dataSourceLimiter);
      dataSourceLimiter.acquire();
    } else {
      if (!dataSourceLimiterMap.get(event.getDataSource()).acquire()) {
        dataSourceRule.removeDataSource(event.getDataSource());
      }
    }
  }

  private class DataSourceLimiter {

    RateLimiter limiter;
    String dataSource;

    private DataSourceLimiter(String dataSource) {
      this.dataSource = dataSource;
      limiter = RateLimiter.create(100.0); // 每秒不超过100个错误
    }

    private boolean acquire() {
      return  limiter.tryAcquire();
    }

  }

  public static void main(String[] args) {
    Map<String, DataSource> dataSourceMap = new HashMap<>();
    dataSourceMap.put("ds_0", null);
    dataSourceMap.put("ds_1", null);
    EventBusInstance.getInstance().register(new SlaveFallbackListener(new DataSourceRule(dataSourceMap)));
    EventBusInstance.getInstance().register(new SlaveFallbackListener(new DataSourceRule(dataSourceMap)));

  }


}
