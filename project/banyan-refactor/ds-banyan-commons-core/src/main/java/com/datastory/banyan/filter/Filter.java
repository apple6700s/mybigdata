package com.datastory.banyan.filter;

import java.io.Serializable;

/**
 * com.datastory.banyan.filter.Filter
 *
 * @author lhfcws
 * @since 16/11/25
 */

public interface Filter extends Serializable {
    /**
     *
     * @param obj
     * @return true to filter the invalid object, false means valid
     */
    public boolean isFiltered(Object obj);
}
