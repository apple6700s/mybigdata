package com.datastory.banyan.newsforum.kafka.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.batch.CombineProcessor;
import com.datastory.banyan.utils.CountUpLatch;

/**
 * com.datastory.banyan.newsforum.kafka.processor.NFTrendCombineProcessor
 *
 * @author lhfcws
 * @since 2017/7/6
 */
public class NFTrendCombineProcessor extends CombineProcessor {
    public NFTrendCombineProcessor(CountUpLatch latch) throws Exception {
        super(latch, NFTrendProcessor.class, NewsForumConsumeProcessor.class);
    }

    public static void main(String[] args) throws Exception {
        String json = "{\"taskId\":\"137876\",\"site\":\"知乎\",\"sourceCrawlerId\":\"5349\",\"post\":\"{\\\"taskId\\\":\\\"137876\\\",\\\"sourceCrawlerId\\\":\\\"5349\\\",\\\"publish_date\\\":\\\"20170711143308\\\",\\\"review_count\\\":\\\"6\\\",\\\"site_id\\\":\\\"-1\\\",\\\"lang\\\":\\\"\\\",\\\"url\\\":\\\"https://www.zhihu.com/question/58376788\\\",\\\"crawler\\\":\\\"5349\\\",\\\"cat_id\\\":\\\"7\\\",\\\"content\\\":\\\"本人92年的，初中开始由dota 后来大学dota2与舍友的推动下玩了LOL 。当然，现在我自己手机里也是有王者荣耀的。 有几个问题，想大家帮帮忙分析的。 1 . 大家网络上对王者荣耀的厌恶，是否类似前些年，大家讨厌LOL，嫌弃其操作，环境，平衡之类的那种感觉类似呢？（感觉现在对LOL有所改观，可能和王者有关，以前很多所说的“小学生”这个词，都转移到王者这个游戏了。） 2. 王者，是真的好玩？（主机那些游戏，先不提，pc端网游也…显示全部\\\",\\\"time_zone\\\":\\\"null\\\",\\\"end_date\\\":\\\"20170710235959\\\",\\\"title\\\":\\\"王者荣耀是否真的那么好玩，还是真的只是方便大家玩？\\\",\\\"item_id\\\":\\\"a8e3fe4b8829f310f91897c22d99a154\\\",\\\"like_count\\\":\\\"0\\\",\\\"page_id\\\":\\\"a8e3fe4b8829f310f91897c22d99a154\\\",\\\"update_date\\\":\\\"20170711143308\\\",\\\"is_main_post\\\":\\\"1\\\",\\\"view_count\\\":\\\"229999\\\",\\\"start_date\\\":\\\"20170704000000\\\"}\",\"replys\":\"[{\\\"taskId\\\":\\\"137876\\\",\\\"sourceCrawlerId\\\":\\\"5349\\\",\\\"publish_date\\\":\\\"20170711143308\\\",\\\"site_id\\\":\\\"-1\\\",\\\"lang\\\":\\\"\\\",\\\"url\\\":\\\"https://www.zhihu.com/question/58376788\\\",\\\"crawler\\\":\\\"5349\\\",\\\"cat_id\\\":\\\"7\\\",\\\"content\\\":\\\"一个东西能成功必然有它的原因的对吗？ 那王者荣耀这么成功除了他好玩还有什么其他必然原因吗？ 操作简单/画面粗暴/易社交/成就系统 不玩这个游戏的我都能说得出这个游戏的乐趣，只是不对我的口味而已。\\\",\\\"time_zone\\\":\\\"null\\\",\\\"author\\\":\\\"黄博\\\",\\\"end_date\\\":\\\"20170710235959\\\",\\\"title\\\":\\\"王者荣耀是否真的那么好玩，还是真的只是方便大家玩？\\\",\\\"like_count\\\":\\\"17\\\",\\\"item_id\\\":\\\"b16a5b66b27f19b7cc879fb10c9918b4\\\",\\\"page_id\\\":\\\"a8e3fe4b8829f310f91897c22d99a154\\\",\\\"update_date\\\":\\\"20170711143308\\\",\\\"is_main_post\\\":\\\"0\\\",\\\"start_date\\\":\\\"20170704000000\\\",\\\"parent_id\\\":\\\"a8e3fe4b8829f310f91897c22d99a154\\\"},{\\\"taskId\\\":\\\"137876\\\",\\\"sourceCrawlerId\\\":\\\"5349\\\",\\\"publish_date\\\":\\\"20170711143308\\\",\\\"site_id\\\":\\\"-1\\\",\\\"lang\\\":\\\"\\\",\\\"url\\\":\\\"https://www.zhihu.com/question/58376788\\\",\\\"crawler\\\":\\\"5349\\\",\\\"cat_id\\\":\\\"7\\\",\\\"content\\\":\\\"妹子表示主要是因为门槛低，操作简单，手游随时可以玩很方便。身边好多妹子都陆续入坑了……要知道从前，moba游戏可是妹子的死敌，专门抢男朋友破坏感情的那种死敌，自从有了农药，多少情侣又多了一个共同爱好啊！\\\",\\\"time_zone\\\":\\\"null\\\",\\\"author\\\":\\\"阿飘\\\",\\\"end_date\\\":\\\"20170710235959\\\",\\\"title\\\":\\\"王者荣耀是否真的那么好玩，还是真的只是方便大家玩？\\\",\\\"like_count\\\":\\\"29\\\",\\\"item_id\\\":\\\"dbff0f97fbd6aa7d086088442e360d0b\\\",\\\"page_id\\\":\\\"a8e3fe4b8829f310f91897c22d99a154\\\",\\\"update_date\\\":\\\"20170711143308\\\",\\\"is_main_post\\\":\\\"0\\\",\\\"start_date\\\":\\\"20170704000000\\\",\\\"parent_id\\\":\\\"a8e3fe4b8829f310f91897c22d99a154\\\"}]\",\"msgDepth\":\"1\",\"site_id\":\"101944\",\"jobName\":\"tencent_strategy_brand_20170711143257_632_86\",\"lang\":\"\",\"crawler\":\"5349\",\"cat_id\":\"7\",\"time_zone\":\"null\",\"end_date\":\"20170710235959\",\"_html_\":\"避免过长，省略...\",\"page_id\":\"a8e3fe4b8829f310f91897c22d99a154\",\"update_date\":\"20170711143308\",\"msgType\":\"1\",\"full_url\":\"https://www.zhihu.com/question/58376788\",\"start_date\":\"20170704000000\"}";
        JSONObject jsonObject = JSON.parseObject(json);
        NFTrendCombineProcessor processor = new NFTrendCombineProcessor(null);
        processor.process(jsonObject);
        processor.cleanup();
    }
}
