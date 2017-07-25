package com.datastory.banyan.weibo.analyz;

import com.datastory.banyan.weibo.analyz.util.Util;
import com.yeezhao.commons.util.FreqDist;
import com.yeezhao.commons.util.StringUtil;

import java.io.Serializable;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * com.datastory.banyan.weibo.analyz.TagDistAnalyzer
 *
 * @author lhfcws
 * @since 2017/1/5
 */
public class TagDistAnalyzer implements Serializable {

    public String analyz(String tags, Iterable<? extends Object> iter) {
        Pattern tagsPattern = Pattern.compile(tags);
        int coveredCount = 0;
        FreqDist<String> tagCounter = new FreqDist<>();
        int size = 0;

        for (Object o : iter) {
            Map<String, String> mp = Util.transform(o);
            String content = mp.get("content");
            if (!StringUtil.isNullOrEmpty(content)) {
                size++;
                Matcher matcher = tagsPattern.matcher(content);
                boolean covered = false;
                while (matcher.find()) {
                    String tag = matcher.group();
                    tagCounter.incr(tag);
                    covered = true;
                }

                if (covered) coveredCount++;
            }
        }

        if (coveredCount == 0)
            return null;

        // 输出格式化
        StringBuilder sb = new StringBuilder();
        sb.append(coveredCount)
                .append(StringUtil.DELIMIT_2ND)
                .append(size);
        for (Map.Entry<String, Integer> e : tagCounter.entrySet()) {
            sb.append(StringUtil.DELIMIT_1ST);
            sb.append(e.getKey());
            sb.append(StringUtil.DELIMIT_2ND);
            sb.append(e.getValue());
        }

        return sb.toString();
    }
}
