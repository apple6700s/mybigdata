package com.datastory.banyan.wechat.valid.runner;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.validate.engine.ValidEngine;
import com.datastory.banyan.validate.engine.impl.XmlValidEngine;
import com.datastory.banyan.validate.entity.ValidResult;
import com.yeezhao.commons.util.ClassUtil;
import com.yeezhao.commons.util.Entity.Params;

import java.io.InputStream;

/**
 * Created by abel.chan on 17/7/18.
 */
public class WechatRunner {
    public static void main(String[] args) {
        try {

            String value = "{\"keywords\":\"尿裤|健康|市区|电话|免费\",\"publish_date\":\"20160727180420\",\"wxid\":\"zzwsxgs\",\"biz\":\"MjM5NjM0ODU1OQ==\",\"url\":\"http://mp.weixin.qq.com/s?__biz=MjM5NjM0ODU1OQ==&mid=2650507734&idx=6&sn=5da17d8a4307d214eaef68cd565fe19c\",\"is_original\":\"0\",\"content\":\"【泉林本色】畅销全球的环保生活纸，0漂白剂添，100%天然秸秆原浆，健康环保更受欢迎，对皮肤来说再也没有比天然零负担清洁萃取来的更重要，在快节奏生活中，健康生活从细节做起，为了家人和宝宝，把健康传递下去！市区内免费送货电话8073808！！ 自由点全系列卫生巾 强势登录枣庄 自由点让您自由随心 轻盈无感！！！极致超薄0.1cm哦！！！全新3D锥型打孔技术 超吸收的双层吸水因子 打孔底模超透气  不漏液 绝对超值值得您拥有！预购从速！市区内免费送货电话8073808！ 要想宝宝好，好之少不了。尿裤谁最牛，好之啥都有。尿裤哪家棒，好之最是当仁不让。好之纸尿裤，妈妈们的最佳选择。妈妈的选择宝宝的健康！市区免费送货电话8073808！！ 喜欢就关注我们！ 市区内免费送货电话8073808！18263222786！ 最终解释权归万事兴公司所有。\",\"fingerprint\":\"0ec9df783261e3ed6ff86fe8224d023c\",\"author\":\"枣庄万事兴公司\",\"sentiment\":\"1\",\"article_id\":\"MjM5NjM0ODU1OQ==:2650507734:6:5da17d8a4307d214eaef68cd565fe19c\",\"title\":\"泉林本色卫生纸，自由点卫生巾，好之纸尿裤 没有比天然更健康的了\",\"thumbnail\":\"http://img01.sogoucdn.com/net/a/04/link?appid=100520033&url=http://mmbiz.qpic.cn/mmbiz/cfbsYtIDBN1ELA2sJmoicJ7XGL1WjCtScCWlOXFgQZATpicqkeQvdXyQHFibFqpjl8OUsQ0PPCANG5ibOkRQnNjRDA/0?wx_fmt=jpeg\",\"like_cnt\":\"0\",\"is_ad\":\"1\",\"update_date\":\"20170125173728\",\"view_cnt\":\"3\",\"brief\":\"【泉林本色】畅销全球的环保生活纸,0漂白剂添,100%天然秸秆原浆,健康环保更受欢迎,对皮肤来说再也没有比天然零负担清洁萃...\",\"pk\":\"67000012c9fb4ca94c517c7c880cfa44\"}";
            JSONObject jsonObject = JSONObject.parseObject(value);
            Params params = new Params();
            for (String s : jsonObject.keySet()) {
                System.out.println(s+"---->"+jsonObject.get(s));
                params.put(s, jsonObject.get(s));
            }
            System.out.println(params);

            //delete
//            params.put("article_id", "article_id");

            //not null
//            params.put("title", "title");
//            params.put("content", "content");
//            params.put("author", "author");
//            params.put("url", "url");
//
//            params.put("biz", "biz");
//            params.put("idx", "idx");
//            params.put("mid", "mid");
//            params.put("sn", "sn");
//            params.put("wxid", "wxid");
////            params.put("wx_author", "wx_author");
//
//            //date
//            params.put("publish_date", "publish_date");
//            params.put("update_date", "update_date");
//
//            //zero
//            params.put("like_cnt", 1);
//            params.put("view_cnt", 0);

            InputStream inputStream = ClassUtil.getResourceAsInputStream("wechat-valid-rule-conf.xml");
            ValidEngine validEngine = XmlValidEngine.getInstance(inputStream);

            ValidResult valudResult = validEngine.execute(params, "ruleset");

            System.out.println(valudResult.toString());
            System.out.println(params);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
