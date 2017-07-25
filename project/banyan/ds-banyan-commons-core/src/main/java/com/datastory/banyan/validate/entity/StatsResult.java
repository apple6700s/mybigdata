package com.datastory.banyan.validate.entity;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by abel.chan on 17/7/19.
 */
public class StatsResult {


    private ConcurrentHashMap<String, Long> docCount = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, Long> successFieldCount = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Long> errorFieldCount = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Long> zeroFieldCount = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Long> nullFieldCount = new ConcurrentHashMap<>();

    int validResultCount = 0;

    public StatsResult() {
    }

    public boolean add(ValidResult validResult) {
        try {

            //doc
            increase(docCount, DocType.ALL.toString());
            if (validResult.isSuccess()) {
                increase(docCount, DocType.NORMAL.toString());
            }

            if (validResult.isErrorDelete()) {
                increase(docCount, DocType.ERROR_DELETE.toString());
            }

            if (validResult.isErrorIgnore()) {
                increase(docCount, DocType.ERROR_IGNORE.toString());
            }

            //field
            parseFields(successFieldCount, validResult.getSuccessFields());

            parseFields(errorFieldCount, validResult.getErrorFields());

            parseFields(zeroFieldCount, validResult.getZeroFields());

            parseFields(nullFieldCount, validResult.getNullFields());

            //记录目前validResult的个数
            validResultCount++;

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean reset() {
        successFieldCount.clear();
        errorFieldCount.clear();
        zeroFieldCount.clear();
        nullFieldCount.clear();

        validResultCount = 0;
        return true;
    }

    private void parseFields(ConcurrentHashMap<String, Long> map, List<String> fields) {
        if (fields != null && fields.size() > 0) {
            for (String field : fields) {
                increase(map, field);
            }
        }
    }

    private long increase(ConcurrentHashMap<String, Long> map, String key) {
        Long oldValue, newValue;
        while (true) {
            oldValue = map.get(key);
            if (oldValue == null) {
                // Add the word firstly, initial the value as 1
                newValue = 1L;
                if (map.putIfAbsent(key, newValue) == null) {
                    break;
                }
            } else {
                newValue = oldValue + 1;
                if (map.replace(key, oldValue, newValue)) {
                    break;
                }
            }
        }
        return newValue;
    }

    public ConcurrentHashMap<String, Long> getDocCount() {
        return docCount;
    }

    public ConcurrentHashMap<String, Long> getSuccessFieldCount() {
        return successFieldCount;
    }

    public ConcurrentHashMap<String, Long> getErrorFieldCount() {
        return errorFieldCount;
    }

    public ConcurrentHashMap<String, Long> getZeroFieldCount() {
        return zeroFieldCount;
    }

    public ConcurrentHashMap<String, Long> getNullFieldCount() {
        return nullFieldCount;
    }

    public int getValidResultCount() {
        return validResultCount;
    }

    @Override
    public String toString() {
        return "StatsResult{" +
                "docCount=" + docCount +
                ", successFieldCount=" + successFieldCount +
                ", errorFieldCount=" + errorFieldCount +
                ", zeroFieldCount=" + zeroFieldCount +
                ", nullFieldCount=" + nullFieldCount +
                ", validResultCount=" + validResultCount +
                '}';
    }
}
