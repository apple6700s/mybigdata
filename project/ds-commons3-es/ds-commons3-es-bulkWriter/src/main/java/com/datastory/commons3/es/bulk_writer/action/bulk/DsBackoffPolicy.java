package com.datastory.commons3.es.bulk_writer.action.bulk;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Iterator;

public class DsBackoffPolicy {

    public static BackoffPolicy boundlessBackoff(int baseInMillis) {
        return new BoundlessBackoff(baseInMillis);
    }

    public static BackoffPolicy boundlessBackoff(TimeValue base) {
        return boundlessBackoff((int) base.millis());
    }

    public static BackoffPolicy boundlessBackoff() {
        return boundlessBackoff(100);
    }

    private static class BoundlessBackoff extends BackoffPolicy {

        private final int baseInMillis;

        public BoundlessBackoff(int baseInMillis) {
            this.baseInMillis = baseInMillis;
        }

        @Override
        public Iterator<TimeValue> iterator() {
            return new BoundlessBackoffIterator(baseInMillis);
        }
    }

    private static class BoundlessBackoffIterator implements Iterator<TimeValue> {

        private static final int[] INCREMENTS = new int[] {
                1, 2, 3, 5, 10, 20, 40, 100, 100, 100, 100, 200, 200,
                300, 300, 500, 500, 800, 1000
        };

        private final int baseInMillis;
        private int curr;

        public BoundlessBackoffIterator(int baseInMillis) {
            this.baseInMillis = baseInMillis;
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public TimeValue next() {
            if (curr < INCREMENTS.length) {
                return TimeValue.timeValueMillis(baseInMillis * INCREMENTS[curr++]);
            } else {
                return TimeValue.timeValueMillis(baseInMillis * INCREMENTS[INCREMENTS.length - 1]);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
