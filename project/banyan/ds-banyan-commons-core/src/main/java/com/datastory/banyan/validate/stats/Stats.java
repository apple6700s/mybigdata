package com.datastory.banyan.validate.stats;

import com.datastory.banyan.validate.entity.StatsResult;
import com.datastory.banyan.validate.entity.ValidResult;

import java.util.List;

/**
 * Created by abel.chan on 17/7/7.
 */
public interface Stats {
    /**
     * 记录单条结果到对应的统计类
     * @param projectName
     * @param validateResult
     * @return
     */
    boolean write(String projectName, ValidResult validateResult);

    /**
     * 批量记录数据到对应的统计类
     * @param projectName
     * @param validateResults
     * @return
     */
    boolean write(String projectName, List<ValidResult> validateResults);


    /**
     * 将统计结果写到redis中
     * @param projectName
     * @param statsResult
     * @return
     */
    boolean write(String projectName, StatsResult statsResult);
}
