package com.dt.mig.sync.words;

import java.util.List;
import java.util.Map;

/**
 * Created by abel.chan on 16/11/20.
 */
public interface IWords {

    public void setUp();

    public Map<String, List<String[]>> getWords();

    public Map<String, String> getOriginWords();

}
