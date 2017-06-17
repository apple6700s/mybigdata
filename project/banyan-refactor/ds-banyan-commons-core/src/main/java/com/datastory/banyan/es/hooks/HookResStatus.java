package com.datastory.banyan.es.hooks;

/**
 * com.datastory.banyan.es.hooks.HookResStatus
 *
 * @author lhfcws
 * @since 16/12/9
 */

public class HookResStatus {
    public static final int SUCCESS = 1;
    public static final int FAIL = -1;
    public static final int RETRY = 0;
    public static final String RETRY_MARK = "__r";
}
