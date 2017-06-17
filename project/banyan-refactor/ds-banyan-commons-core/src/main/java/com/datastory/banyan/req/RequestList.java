package com.datastory.banyan.req;


import com.yeezhao.commons.util.Function;

import java.util.*;

/**
 * com.datastory.banyan.req.RequestList
 *
 * @author lhfcws
 * @since 2017/3/22
 */
public class RequestList<T> extends ArrayList<Request<T>> {

    public RequestList(int initialCapacity) {
        super(initialCapacity);
    }

    public RequestList() {
    }

    public RequestList(Collection<? extends Request<T>> c) {
        super(c);
    }

    public ArrayList<T> toRequestObjs() {
        ArrayList<T> list = new ArrayList<>();
        for (Request<T> r : this) {
            list.add(r.getRequestObj());
        }
        return list;
    }

    public void map(Function<Request<T>, Void> func) {
        for (Request<T> r : this) {
            func.apply(r);
        }
    }

    public void disableRetry() {
        for (Request req : this) {
            req.disableRetry();
        }
    }

    public static <T> RequestList<T> createByRequstObjList(Collection<T> c) {
        RequestList<T> list = new RequestList<>();
        for (T t : c) {
            Request<T> r = new Request<>(t);
            list.add(r);
        }
        return list;
    }

    public static <T> List<Request<T>> createThreadSafeList(Class<T> klass) {
        return Collections.synchronizedList(new ArrayList<Request<T>>());
    }
}
