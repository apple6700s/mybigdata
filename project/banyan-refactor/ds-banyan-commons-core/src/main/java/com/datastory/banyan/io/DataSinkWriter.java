package com.datastory.banyan.io;

import java.io.Serializable;
import java.util.List;

/**
 * com.datastory.banyan.io.DataSinkWriter
 *
 * @author lhfcws
 * @since 16/11/22
 */

public interface DataSinkWriter extends Serializable {
    public List<DataSinkWriteHook> getHooks();

}
