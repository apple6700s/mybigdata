package com.datastory.banyan.newsforum.doc;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.analyz.HTMLTrimmer;
import com.datastory.banyan.doc.JSONObjectDocMapper;
import com.datastory.banyan.newsforum.analyz.NewsForumAnalyzer;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.DateUtils;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * com.datastory.banyan.newsforum.doc.Rhino2NewsForumDocMapper
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class Rhino2NewsForumDocMapper extends JSONObjectDocMapper {
    public Rhino2NewsForumDocMapper(JSONObject jsonObject) {
        super(jsonObject);
    }

    private static final String[] nestedDirectM_ = {
            "cat_id", "update_date",
            "site_id", "url", "title", "author", "content",
            "publish_date", "source", "page_id", "taskId",
            "is_hot", "is_recom", "is_top", "is_digest",
            "introduction", "origin_label", "same_html_count"
    };

    private static final String[] nestedRenameM_ = {
            "review_count", "review_cnt",
            "parent_id", "parent_post_id",
            "view_count", "view_cnt",
            "forum_name", "site_name",
            "item_id", "pk",
            "like_count", "like_cnt",//点赞数
            "dislike_count", "dislike_cnt"//踩数
    };
    private static final Map<String, String> renameM = BanyanTypeUtil.strArr2strMap(nestedRenameM_);

    @Override
    public List<Params> map() {
        ArrayList<Params> ret = new ArrayList<>();

        String json = null;
        JSONObject post = null;
        JSONArray replys = null;
        String siteId = this.jsonObject.getString("site_id");
        String taskId = this.jsonObject.getString("taskId");

        if (this.jsonObject.containsKey("post")) {
            json = getString("post");
            post = JSONObject.parseObject(json);

        }
        if (this.jsonObject.containsKey("replys")) {
            json = getString("replys");
            replys = JSONArray.parseArray(json);
        }

        if (post != null) {
            Params mainPost = parseJsonObject(post);
            if (mainPost != null) {
                mainPost.put("taskId", taskId);
                mainPost.put("is_main_post", "1");
//                 爬虫写错字段导致site_id变成post中的parent_id，临时适应
//                siteId = (String) mainPost.remove("parent_post_id");
//                if (BanyanTypeUtil.validate(siteId))
                mainPost.put("site_id", siteId);
                ret.add(mainPost);
            }
        }

        if (replys != null)
            for (int i = 0; i < replys.size(); i++) {
                JSONObject obj = (JSONObject) replys.get(i);
                Params reply = parseJsonObject(obj);
                if (reply != null && !reply.isEmpty() && reply.get("pk") != null) {
                    reply.put("is_main_post", "0");
                    reply.put("taskId", taskId);
                    if (BanyanTypeUtil.valid(siteId)) {
                        reply.put("site_id", siteId);
                    }
                    ret.add(reply);
                }
            }

        return ret;
    }

    private Params parseJsonObject(JSONObject nested) {
        Params p = new Params();
        if (nested == null)
            return null;

        for (Map.Entry<String, String> e : renameM.entrySet()) {
            String oldField = e.getKey();
            String newField = e.getValue();

            if (nested.containsKey(oldField) && !StringUtils.isEmpty(nested.getString(oldField))) {
                BanyanTypeUtil.safePut(p, newField, nested.getString(oldField));
            }
        }

        if (!BanyanTypeUtil.valid(p.getString("pk")))
            return null;

        for (String field : nestedDirectM_)
            if (BanyanTypeUtil.valid(nested.getString(field)) && nested.containsKey(field)) {
                BanyanTypeUtil.safePut(p, field, nested.getString(field));
            }

        if (!DateUtils.validateDatetime(p.getString("publish_date")))
            p.put("publish_date", null);

        if ("{source}".equals(p.getString("source"))) {
            p.remove("source");
        }

        HTMLTrimmer.trim(p, "content");
        HTMLTrimmer.trim(p, "author");
        HTMLTrimmer.trim(p, "title");

        return p;
    }

    public static void main(String[] args) {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        String json = "{\"cat_id\":\"2\",\"sourceCrawlerId\":\"1527\",\"post\":\"{\\\"taskId\\\":\\\"16818\\\",\\\"sourceCrawlerId\\\":\\\"1527\\\",\\\"is_digest\\\":\\\"0\\\",\\\"publish_date\\\":\\\"20140423160300\\\",\\\"review_count\\\":\\\"2639\\\",\\\"site_id\\\":\\\"100597\\\",\\\"url\\\":\\\"http://bbs.gfan.com/android-7390395-1-1.html\\\",\\\"content\\\":\\\"游戏名称：单机斗地主 游戏版本：v5.2.5 游戏大小：主程序18.6M 支持系统：Android 2.2+ 测试机型：Note2 Android 4.1.1 是否中文：是 游戏介绍 单机斗地主闯关版 天天斗地主，天天都快乐！作为国内顶级棋牌厂商联众游戏2013年的手游扛鼎力作，《单机斗地主》闯关版强势来袭！新增诸多原创功能ge使得《单机斗地主》闯关版成为手机上最好玩、最与众不同的斗地主游戏！ ◆独家特色◆ ★ 超值实惠的抽奖赠送 登陆单机银币奖励、赢话费赛事活动，更有每天万元充值卡\\\\u0026钻石抽奖等你挑战； ★ 丰富刺激的挑战关卡 增加了游戏的挑战性，通关的满足感成倍提升，让人越发欲罢不能； ★ 全新绘制的美术风格 清新大气的高清画面可谓颠覆从前，媲美PC，给予你美妙的视觉享受； ★ 新鲜特色的场景闯关 每个场景都能带来全新的体验与无穷的乐趣，且通关更有多多奖励； ★ 更多丰富的特殊道具 游戏内容和玩法更加丰富，层出不穷的新鲜感让乐趣持久更耐玩； ★ 便捷多样的登录方式 支持游客、联众、人人、百度等多种账号登陆，方便新手快速进入体验游戏； ★ 智能升级的单机AI 毫不逊色真人PK，不联网也能得到极富挑战性的单机游戏体验！ 新版变化 更新内容： 1新春活动闪亮开启，参与就送小米3、土豪金，更有神秘大奖等着你！ 2新增小丑游戏，亿万金币池等你来拿 3 新增记牌器等会员专属福利 4 公告、签到等细节全面优化 游戏截图 游戏下载 游客，如果您要查看本帖隐藏内容请回复或者点击我就看看，不说话 更多关于我发布的游戏资源还请点击zy11577\\\",\\\"cat_id\\\":\\\"2\\\",\\\"author\\\":\\\"zy11577\\\",\\\"title\\\":\\\"单机斗地主 v5.2.5中文版更新 极富挑战性的单机游戏体验\\\",\\\"item_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\",\\\"is_top\\\":\\\"0\\\",\\\"page_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\",\\\"update_date\\\":\\\"20161122000601\\\",\\\"is_recom\\\":\\\"0\\\",\\\"is_hot\\\":\\\"0\\\",\\\"is_main_post\\\":\\\"1\\\",\\\"view_count\\\":\\\"68441\\\"}\",\"_html_\":\"避免过长，省略...\",\"replys\":\"[{\\\"taskId\\\":\\\"16818\\\",\\\"sourceCrawlerId\\\":\\\"1527\\\",\\\"is_digest\\\":\\\"0\\\",\\\"publish_date\\\":\\\"20140423162300\\\",\\\"site_id\\\":\\\"100597\\\",\\\"url\\\":\\\"http://bbs.gfan.com/android-7390395-1-1.html\\\",\\\"content\\\":\\\"555555555555555555\\\",\\\"cat_id\\\":\\\"2\\\",\\\"author\\\":\\\"迷途小书生\\\",\\\"title\\\":\\\"单机斗地主 v5.2.5中文版更新 极富挑战性的单机游戏体验\\\",\\\"item_id\\\":\\\"044adf84c840805e6276ad5e8593970e\\\",\\\"is_top\\\":\\\"0\\\",\\\"page_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\",\\\"update_date\\\":\\\"20161122000601\\\",\\\"is_recom\\\":\\\"0\\\",\\\"is_hot\\\":\\\"0\\\",\\\"is_main_post\\\":\\\"0\\\",\\\"parent_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\"},{\\\"taskId\\\":\\\"16818\\\",\\\"sourceCrawlerId\\\":\\\"1527\\\",\\\"is_digest\\\":\\\"0\\\",\\\"publish_date\\\":\\\"20140423163400\\\",\\\"site_id\\\":\\\"100597\\\",\\\"url\\\":\\\"http://bbs.gfan.com/android-7390395-1-1.html\\\",\\\"cat_id\\\":\\\"2\\\",\\\"author\\\":\\\"haoh2718\\\",\\\"title\\\":\\\"单机斗地主 v5.2.5中文版更新 极富挑战性的单机游戏体验\\\",\\\"item_id\\\":\\\"c12c561b7ed112ea0790ee58f12f8219\\\",\\\"is_top\\\":\\\"0\\\",\\\"page_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\",\\\"update_date\\\":\\\"20161122000601\\\",\\\"is_recom\\\":\\\"0\\\",\\\"is_hot\\\":\\\"0\\\",\\\"is_main_post\\\":\\\"0\\\",\\\"parent_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\"},{\\\"taskId\\\":\\\"16818\\\",\\\"sourceCrawlerId\\\":\\\"1527\\\",\\\"is_digest\\\":\\\"0\\\",\\\"publish_date\\\":\\\"20140423164400\\\",\\\"site_id\\\":\\\"100597\\\",\\\"url\\\":\\\"http://bbs.gfan.com/android-7390395-1-1.html\\\",\\\"cat_id\\\":\\\"2\\\",\\\"author\\\":\\\"eq4321\\\",\\\"title\\\":\\\"单机斗地主 v5.2.5中文版更新 极富挑战性的单机游戏体验\\\",\\\"item_id\\\":\\\"c3959bbe3befc830c81ad7174ea3209a\\\",\\\"is_top\\\":\\\"0\\\",\\\"page_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\",\\\"update_date\\\":\\\"20161122000601\\\",\\\"is_recom\\\":\\\"0\\\",\\\"is_hot\\\":\\\"0\\\",\\\"is_main_post\\\":\\\"0\\\",\\\"parent_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\"},{\\\"taskId\\\":\\\"16818\\\",\\\"sourceCrawlerId\\\":\\\"1527\\\",\\\"is_digest\\\":\\\"0\\\",\\\"publish_date\\\":\\\"20140423165100\\\",\\\"site_id\\\":\\\"100597\\\",\\\"url\\\":\\\"http://bbs.gfan.com/android-7390395-1-1.html\\\",\\\"content\\\":\\\"感谢分享这么好的软件\\\",\\\"cat_id\\\":\\\"2\\\",\\\"author\\\":\\\"凌志飞翔\\\",\\\"title\\\":\\\"单机斗地主 v5.2.5中文版更新 极富挑战性的单机游戏体验\\\",\\\"item_id\\\":\\\"76868b400425b632099fec0e19522581\\\",\\\"is_top\\\":\\\"0\\\",\\\"page_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\",\\\"update_date\\\":\\\"20161122000601\\\",\\\"is_recom\\\":\\\"0\\\",\\\"is_hot\\\":\\\"0\\\",\\\"is_main_post\\\":\\\"0\\\",\\\"parent_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\"},{\\\"taskId\\\":\\\"16818\\\",\\\"sourceCrawlerId\\\":\\\"1527\\\",\\\"is_digest\\\":\\\"0\\\",\\\"publish_date\\\":\\\"20140423165300\\\",\\\"site_id\\\":\\\"100597\\\",\\\"url\\\":\\\"http://bbs.gfan.com/android-7390395-1-1.html\\\",\\\"content\\\":\\\"第一次下游戏，不知什么好玩。\\\",\\\"cat_id\\\":\\\"2\\\",\\\"author\\\":\\\"小肥肥xff\\\",\\\"title\\\":\\\"单机斗地主 v5.2.5中文版更新 极富挑战性的单机游戏体验\\\",\\\"item_id\\\":\\\"21b208e47cc71a990bb555271e4a0402\\\",\\\"is_top\\\":\\\"0\\\",\\\"page_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\",\\\"update_date\\\":\\\"20161122000601\\\",\\\"is_recom\\\":\\\"0\\\",\\\"is_hot\\\":\\\"0\\\",\\\"is_main_post\\\":\\\"0\\\",\\\"parent_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\"},{\\\"taskId\\\":\\\"16818\\\",\\\"sourceCrawlerId\\\":\\\"1527\\\",\\\"is_digest\\\":\\\"0\\\",\\\"publish_date\\\":\\\"20140423165300\\\",\\\"site_id\\\":\\\"100597\\\",\\\"url\\\":\\\"http://bbs.gfan.com/android-7390395-1-1.html\\\",\\\"content\\\":\\\"谢谢LZ的分享\\\",\\\"cat_id\\\":\\\"2\\\",\\\"author\\\":\\\"Saga_me\\\",\\\"title\\\":\\\"单机斗地主 v5.2.5中文版更新 极富挑战性的单机游戏体验\\\",\\\"item_id\\\":\\\"b829cd8d36941d55542ab971b542ead7\\\",\\\"is_top\\\":\\\"0\\\",\\\"page_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\",\\\"update_date\\\":\\\"20161122000601\\\",\\\"is_recom\\\":\\\"0\\\",\\\"is_hot\\\":\\\"0\\\",\\\"is_main_post\\\":\\\"0\\\",\\\"parent_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\"},{\\\"taskId\\\":\\\"16818\\\",\\\"sourceCrawlerId\\\":\\\"1527\\\",\\\"is_digest\\\":\\\"0\\\",\\\"publish_date\\\":\\\"20140423165800\\\",\\\"site_id\\\":\\\"100597\\\",\\\"url\\\":\\\"http://bbs.gfan.com/android-7390395-1-1.html\\\",\\\"content\\\":\\\"感谢楼主分享\\\",\\\"cat_id\\\":\\\"2\\\",\\\"author\\\":\\\"苍穹遮天\\\",\\\"title\\\":\\\"单机斗地主 v5.2.5中文版更新 极富挑战性的单机游戏体验\\\",\\\"item_id\\\":\\\"5179684eb813595d80743273eb2a1672\\\",\\\"is_top\\\":\\\"0\\\",\\\"page_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\",\\\"update_date\\\":\\\"20161122000601\\\",\\\"is_recom\\\":\\\"0\\\",\\\"is_hot\\\":\\\"0\\\",\\\"is_main_post\\\":\\\"0\\\",\\\"parent_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\"},{\\\"taskId\\\":\\\"16818\\\",\\\"sourceCrawlerId\\\":\\\"1527\\\",\\\"is_digest\\\":\\\"0\\\",\\\"publish_date\\\":\\\"20140423170400\\\",\\\"site_id\\\":\\\"100597\\\",\\\"url\\\":\\\"http://bbs.gfan.com/android-7390395-1-1.html\\\",\\\"content\\\":\\\"最好的单机斗地主了\\\",\\\"cat_id\\\":\\\"2\\\",\\\"author\\\":\\\"relax9980\\\",\\\"title\\\":\\\"单机斗地主 v5.2.5中文版更新 极富挑战性的单机游戏体验\\\",\\\"item_id\\\":\\\"1e35124d3b7786b5a99ffb649c12b1ec\\\",\\\"is_top\\\":\\\"0\\\",\\\"page_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\",\\\"update_date\\\":\\\"20161122000601\\\",\\\"is_recom\\\":\\\"0\\\",\\\"is_hot\\\":\\\"0\\\",\\\"is_main_post\\\":\\\"0\\\",\\\"parent_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\"},{\\\"taskId\\\":\\\"16818\\\",\\\"sourceCrawlerId\\\":\\\"1527\\\",\\\"is_digest\\\":\\\"0\\\",\\\"publish_date\\\":\\\"20140423171500\\\",\\\"site_id\\\":\\\"100597\\\",\\\"url\\\":\\\"http://bbs.gfan.com/android-7390395-1-1.html\\\",\\\"content\\\":\\\"进来看看\\\",\\\"cat_id\\\":\\\"2\\\",\\\"author\\\":\\\"andywen151\\\",\\\"title\\\":\\\"单机斗地主 v5.2.5中文版更新 极富挑战性的单机游戏体验\\\",\\\"item_id\\\":\\\"7189b89920df6bf762346a1b776b8582\\\",\\\"is_top\\\":\\\"0\\\",\\\"page_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\",\\\"update_date\\\":\\\"20161122000601\\\",\\\"is_recom\\\":\\\"0\\\",\\\"is_hot\\\":\\\"0\\\",\\\"is_main_post\\\":\\\"0\\\",\\\"parent_id\\\":\\\"a554a532c0c5bbacc9d07c372c2aeb5b\\\"}]\",\"msgDepth\":\"2\",\"page_id\":\"a554a532c0c5bbacc9d07c372c2aeb5b\",\"update_date\":\"20161122000601\",\"site_id\":\"100597\",\"msgType\":\"1\",\"full_url\":\"http://bbs.gfan.com/forum-170-1.html\",\"jobName\":\"increment_job_20161122000549_482_75\"}";
        JSONObject jsonObject = JSONObject.parseObject(json);
        List<Params> list = new Rhino2NewsForumDocMapper(jsonObject).map();
        for (Params p : list) {
            p = NewsForumAnalyzer.getInstance().analyz(p);
            System.out.println(BanyanTypeUtil.prettyStringifyMap(p));
        }
        System.out.println(list.size());

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
