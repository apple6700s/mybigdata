package com.dt.mig.sync.utils;

import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.FreqDist;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.commons.util.encypt.Md5Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import scala.collection.Seq;

import java.util.*;

/**
 * com.datastory.banyan.utils.BanyanTypeUtil
 *
 * @author lhfcws
 * @since 2016/10/21
 */
public class BanyanTypeUtil {
    public static Random random = new Random();

    public static void copyField(Map<String, Object> in, Map<String, Object> out, String field) {
        out.put(field, in.get(field));
    }

    public static String incString(String numStr, long value) {
        Long num = parseLong(numStr);
        if (num == null) return numStr;
        else return String.valueOf(num + value);
    }

    public static Integer lenOrNull(String s) {
        if (s == null) return null;
        return s.length();
    }

    public static Integer len(String s) {
        if (s == null) return 0;
        return s.length();
    }

    public static byte[] bytes(String s) {
        if (s == null) return null;
        return s.getBytes();
    }

    public static void putAllNotNull(Map<String, Object> out, Map<String, Object> in) {
        if (in != null) for (Map.Entry<String, Object> e : in.entrySet()) {
            if (e.getValue() != null) out.put(e.getKey(), e.getValue());
        }
    }

    public static void putAllNotNull(Map<String, Object> out, Map<String, Object> in, String[] fields) {
        if (in != null) {
            for (String field : fields) {
                if (in.get(field) != null) {
                    out.put(field, in.get(field));
                }
            }
        }
    }

    public static void putAllNotNull(Map<String, Object> out, Map<String, Object> in, Map<String, String> mappingFields) {
        if (in != null) {
            for (Map.Entry<String, String> e : mappingFields.entrySet()) {
                if (in.get(e.getKey()) != null) {
                    out.put(e.getValue(), in.get(e.getKey()));
                }
            }
        }
    }

    public static void putShortNotNull(Map<String, Object> out, Map<String, Object> in, String[] fields) {
        if (in != null) {
            for (String field : fields) {
                if (in.get(field) != null) {
                    Short sht = parseShort(in.get(field));
                    if (sht != null) out.put(field, sht);
                }
            }
        }
    }

    public static void putYzListNotNull(Map<String, Object> out, Map<String, Object> in, String[] fields) {
        if (in != null) {
            for (String field : fields) {
                if (in.get(field) != null) {
                    try {
                        List<String> list = yzStr2List((String) in.get(field));
                        if (list != null) out.put(field, list);
                    } catch (Exception ignore) {
                        ignore.printStackTrace();
                    }
                }
            }
        }
    }

    public static int[] newArray(int size, int value) {
        int[] ret = new int[size];
        Arrays.fill(ret, value);
        return ret;
    }

    public static <T> T[] shuffleArray(T[] arr) {
        if (arr == null || arr.length < 2) return arr;
        int len = arr.length;
        for (int i = len; i > 1; i--) {
            int j = i;
            while (i == j) {
                j = random.nextInt(i);
            }
            swapArrayElements(arr, i - 1, j);
        }
        return arr;
    }

    public static <T> T[] shuffleCopyArray(T[] arr) {
        if (arr == null || arr.length < 2) return arr;
        int len = arr.length;
        T[] ret = Arrays.copyOf(arr, len);
        for (int i = len; i > 1; i--) {
            int j = i;
            while (i == j) {
                j = random.nextInt(i);
            }
            swapArrayElements(ret, i - 1, j);
        }
        return ret;
    }

    public static <T> T[] swapArrayElements(T[] arr, int i, int j) {
        T t = arr[i];
        arr[i] = arr[j];
        arr[j] = t;
        return arr;
    }

    /**
     * O(n * len(list))
     *
     * @param list
     * @param n
     * @param <T>
     * @return
     */
    public static List<String> topN(List<String> list, int n) {
        boolean[] visit = new boolean[list.size()];

        List<String> ret = new ArrayList<>();
        while (ret.size() < n && ret.size() < list.size()) {
            int maxI = -1;
            String c = null;
            int i = 0;
            for (String o : list) {
                if (c == null || c.compareTo(o) > 0) {
                    if (!visit[i]) {
                        c = o;
                        maxI = i;
                    }
                }
                i++;
            }
            visit[maxI] = true;
            ret.add(c);
        }
        return ret;
    }

    public static <T> T randomPickArray(T[] arr) {
        if (arr == null || arr.length == 0) return null;
        int n = random.nextInt(arr.length);
        return arr[n];
    }

    public static void safePut(Map<String, Object> p, String key, Object value) {
        if (value != null) p.put(key, value);
    }

    public static String merge(Collection<String> collection) {
        if (collection == null || collection.isEmpty()) return null;
        return StringUtils.join(collection, StringUtil.DELIMIT_1ST);
    }

    public static StrParams byte2strMap(Map<byte[], byte[]> mp) {
        StrParams strParams = new StrParams();
        for (Map.Entry<byte[], byte[]> e : mp.entrySet()) {
            strParams.put(Bytes.toString(e.getKey()), Bytes.toString(e.getValue()));
        }
        return strParams;
    }

    public static List<String> getMapStrKeys(Map<byte[], byte[]> mp) {
        List<String> list = new LinkedList<>();
        for (byte[] b : mp.keySet()) {
            list.add(Bytes.toString(b));
        }
        return list;
    }

    /**
     * Assuming that arr.length is eval
     *
     * @param arr
     * @return
     */
    public static StrParams strArr2strMap(String[] arr) {
        StrParams p = new StrParams();
        if (arr != null && arr.length % 2 == 0) for (int i = 0; i < arr.length; i += 2) {
            p.put(arr[i], arr[i + 1]);
        }
        return p;
    }

    public static Integer parseInt(Object obj) {
        try {
            String str = String.valueOf(obj);
            return Integer.parseInt(str);
        } catch (Exception ignore) {
            return null;
        }
    }

    public static Integer parseIntForce(Object obj) {
        try {
            String str = String.valueOf(obj);
            return Integer.parseInt(str);
        } catch (Exception ignore) {
            return 0;
        }
    }

    public static Long parseLong(Object obj) {
        try {
            String str = String.valueOf(obj);
            return Long.parseLong(str);
        } catch (Exception ignore) {
            return null;
        }
    }

    public static Long parseLongForce(Object obj) {
        try {
            String str = String.valueOf(obj);
            return Long.parseLong(str);
        } catch (Exception ignore) {
            return 0l;
        }
    }

    public static Short parseShort(Object obj) {
        try {
            String str = String.valueOf(obj);
            return Short.parseShort(str);
        } catch (Exception ignore) {
            return null;
        }
    }

    public static String parseString(Object obj) {
        try {
            if (obj == null) return null;
            String str = String.valueOf(obj);
            return str;
        } catch (Exception ignore) {
            return null;
        }
    }


    public static Short parseShortForce(Object obj) {
        try {
            String str = String.valueOf(obj);
            return Short.parseShort(str);
        } catch (Exception ignore) {
            return 0;
        }
    }

    public static List<String> yzStr2List(String v) {
        if (StringUtil.isNullOrEmpty(v)) return null;
        String[] items = v.split(StringUtil.STR_DELIMIT_1ST);
        List<String> vList = new ArrayList<>();
        for (String item : items) {
            String[] entries = item.split(StringUtil.STR_DELIMIT_2ND);
            if (entries.length == 0) continue;
            vList.add(entries[0]);
        }
        return vList;
    }

    public FreqDist<Integer> intArr2FreqDist(int[] arr) {
        FreqDist<Integer> freqDist = new FreqDist<>();
        if (arr != null) for (int a : arr)
            freqDist.inc(a);
        return freqDist;
    }

    public static List<String> sortStrMapKeys(Map<String, ? extends Object> mp) {
        List<String> keys = new LinkedList<>(mp.keySet());
        Collections.sort(keys);
        return keys;
    }

    public static String list2yzStr(List<String> list) {
        return StringUtils.join(list, "|");
    }

    public static String prettyStringifyMap(Map<? extends Object, ? extends Object> mp) {
        StringBuilder sb = new StringBuilder();
        if (mp != null) for (Map.Entry<? extends Object, ? extends Object> e : mp.entrySet()) {
            sb.append(e.getKey()).append(" = ").append(e.getValue()).append("\n");
        }
        return sb.toString();
    }

    public static String prettySortStringifyMap(Map<? extends Comparable, ? extends Object> mp) {
        StringBuilder sb = new StringBuilder();
        if (mp != null) {
            List<? extends Comparable> list = new ArrayList<>(mp.keySet());
            Collections.sort(list);
            for (Comparable key : list) {
                sb.append("  ").append(key).append(" = ").append(mp.get(key)).append("\n");
            }
        }
        return sb.toString();
    }

    public static String prettyStringifyList(List<? extends Object> list) {
        StringBuilder sb = new StringBuilder();
        if (list != null) for (Object obj : list) {
            sb.append(obj).append("\n");
        }
        return sb.toString();
    }

    public static String prettySortStringifyList(List<? extends Comparable> list) {
        Collections.sort(list);
        StringBuilder sb = new StringBuilder();
        if (list != null) for (Object obj : list) {
            sb.append(obj).append("\n");
        }
        return sb.toString();
    }

    public static String joinNWrap(Collection<String> collection, String sep, String beginWrap, String endWrap) {
        StringBuilder sb = new StringBuilder();
        for (String s : collection) {
            sb.append(beginWrap).append(s).append(endWrap).append(sep);
        }
        if (sb.length() > 0) sb.setLength(sb.length() - sep.length());
        return sb.toString();
    }

    public static String repeat(String segment, int times, String sep) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < times; i++) {
            sb.append(segment);
            if (sep != null) sb.append(sep);
        }
        if (StringUtils.isNotEmpty(sep) && sb.length() > 0) {
            sb.setLength(sb.length() - sep.length());
        }
        return sb.toString();
    }

    public static String sub3PK(String id) {
        return Md5Util.md5(id).substring(0, 3) + id;
    }

    public static String sub2PK(String id) {
        return Md5Util.md5(id).substring(0, 2) + id;
    }

    /**
     * HBase wbuserPK key
     *
     * @param uid
     * @return
     */
    public static String wbuserPK(String uid) {
//        return Md5Util.md5(uid).substring(0, 2) + uid;
        return sub3PK(uid);
    }

    public static String wbcontentPK(String mid) {
        return sub3PK(mid);
    }

    public static Set<String> arr2Set(String[] arr) {
        return new HashSet<>(Arrays.asList(arr));
    }

    public static <T> Seq<T> toSeq(List<T> list) {
        Seq<T> seq = scala.collection.JavaConversions.asScalaBuffer(list);
        return seq;
    }
}
