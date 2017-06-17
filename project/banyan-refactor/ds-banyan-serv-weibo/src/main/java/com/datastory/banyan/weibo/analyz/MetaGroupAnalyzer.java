package com.datastory.banyan.weibo.analyz;

import com.datastory.banyan.base.RhinoETLConsts;
import com.datatub.scavenger.base.MgroupConsts;
import com.datatub.scavenger.model.TagDist;
import com.datatub.scavenger.util.CollectionsUtil;
import com.datatub.scavenger.util.MathUtil;
import com.yeezhao.commons.util.DoubleDist;
import com.yeezhao.commons.util.FreqDist;
import com.yeezhao.commons.util.StringUtil;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * com.datastory.banyan.weibo.analyz.MetaGroupAnalyzer
 *
 * @author Leslie
 * @since 2016/10/21
 */
public class MetaGroupAnalyzer {
    private static final int MAX_LEN = RhinoETLConsts.MAX_ANALYZ_LEN;
    private static final Logger LOG = Logger.getLogger(MetaGroupAnalyzer.class);
    public static final String SELF_PREFIX = "_";

    private int alpha = MgroupConsts.DEFAULT_ALPHA; // alpha - 平滑函数，用以平滑那些tag在用户微博中没有出现的情况
    private int minOcc = MgroupConsts.DEFALUT_MIN_OCC; // 最少出现的 tag 次
    private double threshold = MgroupConsts.DEFAULT_THRESHOLD; // 权重小于0.01的tag会被过滤掉

    public int getAlpha() {
        return alpha;
    }

    public void setAlpha(int alpha) {
        this.alpha = alpha;
    }

    public int getMinOcc() {
        return minOcc;
    }

    public void setMinOcc(int minOcc) {
        this.minOcc = minOcc;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    /**
     * 基于用户的单向关注信息，预测用户的兴趣标签。
     * 设计逻辑：
     * 1. 获取自身以及一定粉丝数范围内的单向关注用户的tag_dist
     * 2. 基于每个tag_dist，计算每个标签对用户兴趣的贡献值。
     * confidence = tweets_covered_count / tweets_total_count
     * p(tag,user) = confidence * (alpha + tag_occurences) / (n * alpha + total_tag_occurences)， 其中n表示tag_dist中有多少个标签
     * 3. 累加自己以及所有用户的所有标签权值
     * 4. 输出结果
     * <p>
     * 输出的格式：cs$ca|g1$occ1$w1|g2$occ2$w2|...  （列表按w降序排序）
     * cs=0表示没有累加用户自身标签，cs=1表示累加了用户自身标签
     * ca表示总共累加的有效的单向关注用户标签的数目，不含自身标签
     * g：表示某些meta label，与MR2结合的话，就是tag
     * occ：表示某个meta label出现的频数
     * w：表示某个meta label相对于用户的权值（保留3位小数点,权值＜0.01的标签全部过滤掉。）
     * <p>
     * 参数说明:
     * alpha - 平滑函数，用以平滑那些tag在用户微博中没有出现的情况
     * flw_rng - 单向关注的粉丝数范围
     * min_occ - 最少出现的tag次数
     *
     * @param selfTagDist   需计算metaGroup的用户的 tag_dist
     * @param followTagDist 用户单向关注的人的 tag_dist列表
     * @return cs$ca|g1$occ1$w1|g2$occ2$w2|...  （列表按w降序排序）
     */
    public String analyz(String selfTagDist, Iterable<String> followTagDist) throws Exception {
        FreqDist<String> tagOccs = new FreqDist<>();   //  标签出现的频次
        DoubleDist<String> tagWeights = new DoubleDist<String>();  //　标签

        int accSelf = 0;
        try {
            // 计算自身的标签权重
            TagDist selfTag = new TagDist().parse(selfTagDist);
            addUserTagDist(selfTag, tagOccs, tagWeights);
            accSelf = 1;
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }

        int accumulated = 0;
        // 计算单向关注列表的标签权重
        for (String tagDistStr : followTagDist) {
            if (tagDistStr == null) continue;
            try {
                TagDist tagDist = new TagDist().parse(tagDistStr);
                addUserTagDist(tagDist, tagOccs, tagWeights);
                accumulated++;
            } catch (Exception e) {
                LOG.error(e.getMessage());
            }
        }

        if (accSelf == 0 && accumulated == 0) {
            return null;
        }

        // 输出格式
        StringBuilder sb = new StringBuilder();
        sb.append(accSelf).append(StringUtil.DELIMIT_2ND).append(accumulated);
        List<Map.Entry<String, Double>> list = new ArrayList<>(tagWeights.entrySet());
        Collections.sort(list, new CollectionsUtil.EntryValueComparator());
        for (Map.Entry<String, Double> e : list) {
            if (tagOccs.getCount(e.getKey()) >= minOcc
                    && e.getValue() >= threshold) {
                sb.append(StringUtil.DELIMIT_1ST).append(e.getKey())
                        .append(StringUtil.DELIMIT_2ND)
                        .append(tagOccs.get(e.getKey()).toString())
                        .append(StringUtil.DELIMIT_2ND)
                        .append(String.format("%.3f", e.getValue()));
            }
        }

        return sb.toString();
    }

    /**
     * added by Lhfcws
     *
     * @param tagDists
     * @return
     */
    public String analyz(Iterable tagDists) {
        if (tagDists == null)
            return null;

        FreqDist<String> tagFreq = new FreqDist<>();   //  标签出现的频次
        DoubleDist<String> tagWeights = new DoubleDist<>();

        int accumulated = 0;
        String selfTagDist = null;
        // 计算单向关注列表的标签权重
        for (Object tagDistStrObj : tagDists) {
            if (tagDistStrObj == null) continue;
            String tagDistStr = tagDistStrObj.toString();
            if (tagDistStr.startsWith(SELF_PREFIX)) {
                selfTagDist = tagDistStr.replace(SELF_PREFIX, "");
                continue;
            }

            try {
                TagDist tagDist = new TagDist().parse(tagDistStr);
                addUserTagDist(tagDist, tagFreq, tagWeights);
                accumulated++;
            } catch (Exception e) {
                LOG.error("[Other tag dist] " + e.getMessage());
            }
        }

        int accSelf = 0;
        if (selfTagDist != null)
            try {
                // 计算自身的标签权重
                TagDist selfTag = new TagDist().parse(selfTagDist);
                addUserTagDist(selfTag, tagFreq, tagWeights);
                accSelf = 1;
            } catch (Exception e) {
                LOG.error("[Self tag dist] " + e.getMessage());
            }

        if (accumulated == 0) {
            return selfTagDist;
        }

        // 输出格式
        StringBuilder sb = new StringBuilder();
        sb.append(accSelf).append(StringUtil.DELIMIT_2ND).append(accumulated);
        List<Map.Entry<String, Double>> list = new ArrayList<>();
        for (Map.Entry<String, Double> e : tagWeights.entrySet()) {
            if (tagFreq.getCount(e.getKey()) >= minOcc && e.getValue() >= threshold) {
                list.add(e);
            }
        }

        Collections.sort(list, new CollectionsUtil.EntryValueComparator());
        int i = 0;
        for (Map.Entry<String, Double> e : list) {
            i++;
            if (i >= MAX_LEN) break;
            sb.append(StringUtil.DELIMIT_1ST).append(e.getKey())
                    .append(StringUtil.DELIMIT_2ND)
                    .append(tagFreq.get(e.getKey()))
                    .append(StringUtil.DELIMIT_2ND)
                    .append(String.format("%.3f", e.getValue()));
        }
        list.clear();

        return sb.toString();
    }

    /**
     * @param tagDist    其实只要他的 confidence
     * @param tagOcc     follow的用户tagDist
     * @param tagWeights 对于当前用户的全局性的tagWeights
     */
    private void addUserTagDist(TagDist tagDist, FreqDist<String> tagOcc, DoubleDist<String> tagWeights) {
        Map<String, Integer> tagCount = tagDist.getTagCount();
        int sum = alpha * tagCount.size() + MathUtil.sum(tagCount.values());

        tagOcc.addAll(tagCount.keySet());

        double confidence = tagDist.getConfidence();
        for (String s : tagCount.keySet()) {
            double weight = confidence * (alpha + tagCount.get(s)) / sum;
            tagWeights.inc(s, weight);
        }
    }
}