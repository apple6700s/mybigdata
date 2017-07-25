package com.datastory.banyan.filter;

/**
 * com.datastory.banyan.filter.AbstractFilter
 *
 * @author lhfcws
 * @since 16/11/25
 */

public abstract class AbstractFilter implements Filter {
    @Override
    /**
     *
     * @param obj
     * @return true to filter the invalid object, false means validate
     */
    public boolean isFiltered(Object obj) {
        try {
            return filter(obj);
        } catch (Throwable e) {
            return true;
        }
    }

    /**
     *
     * @param obj
     * @return true to filter the invalid object, false means validate
     */
    public abstract boolean filter(Object obj) throws Throwable;
}
