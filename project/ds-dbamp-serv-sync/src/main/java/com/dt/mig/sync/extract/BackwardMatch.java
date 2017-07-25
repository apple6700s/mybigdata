package com.dt.mig.sync.extract;

import java.util.*;

/**
 * 最长正想匹配算法
 * <p>
 * Created by abel.chan on 16/12/21.
 */
public class BackwardMatch {

    public Set<String> leftMax(String sentence, List<String> whiteList) {
        Set<String> result = new HashSet<String>();
        String s = "";
        for (int i = 0; i < sentence.length(); i++) {
            s += sentence.charAt(i);
            if (isIn(s, whiteList) && aheadCount(s, whiteList) == 1) {
                result.add(s);
                s = "";
            } else if (aheadCount(s, whiteList) > 0) {

            } else {
                s = "";
            }
        }
        return result;
    }

    private boolean isIn(String s, List<String> list) {
        for (int i = 0; i < list.size(); i++) {
            if (s.equals(list.get(i))) return true;
        }
        return false;
    }

    private int aheadCount(String s, List<String> list) {
        int count = 0;
        for (int i = 0; i < list.size(); i++) {
            if ((s.length() <= list.get(i).length()) && (s.equals(list.get(i).substring(0, s.length())))) count++;
        }
        return count;
    }


    public List<String> backwardMatch(String s, List<String> words, int len) {
        List<String> result = new ArrayList<>();
        Map<String, List<String>> map = new HashMap<String, List<String>>();
        int prefixLen = len;
        for (String word : words) {
            if (word != null) {
                prefixLen = Math.min(prefixLen, word.length());
            }
        }
        for (String word : words) {
            String prefix = word.substring(0, prefixLen);
            List<String> list;
            if (!map.containsKey(prefix)) {
                list = new ArrayList<String>();
                map.put(prefix, list);
            } else {
                list = map.get(prefix);
            }
            list.add(word);
        }
        char[] chars = s.toCharArray();
        for (int i = 0; i < s.length(); i++) {
            char[] val = new char[prefixLen];
            for (int j = 0; j < prefixLen && i + j < s.length(); j++) {
                val[j] = chars[i + j];
            }
            String name = new String(val);
            if (map.containsKey(name)) {
                List<String> list = map.get(name);
                for (int k = list.size() - 1; k >= 0; k--) {

                    char[] val2 = list.get(k).toCharArray();
                    if (prefixLen == val2.length) {
                        String v = list.remove(k);
                        result.add(v);
                        if (list.size() == 0) {
                            map.remove(name);
                        }
                        continue;
                    }
                    int g = 0;
                    while (val2[g + prefixLen] == chars[i + prefixLen + g]) {
                        g++;
                        if (g == val2.length - prefixLen) {
                            String v = list.remove(k);
                            result.add(v);
                            if (list.size() == 0) {
                                map.remove(name);
                            }
                            break;
                        }
                    }
                }
            }
        }
        return result;
    }

}
