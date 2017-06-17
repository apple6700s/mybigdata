package com.datastory.banyan.io;

import java.util.Iterator;
import java.util.List;

/**
 * com.datastory.banyan.io.DataSourceReader
 *
 * @author lhfcws
 * @since 16/11/22
 */

public interface DataSourceReader {
    public List<DataSrcReadHook> getHooks();

    public Object read(Object input) throws Exception;

    public List<? extends Object> reads(Iterator iterator) throws Exception;
}
