package com.datastory.banyan.schema;

import com.datastory.banyan.schema.annotation.Ch;
import com.datastory.banyan.schema.annotation.Type;

/**
 * com.datastory.banyan.schema.PhWeiboUserSchema
 *
 * @author lhfcws
 * @since 2017/5/19
 */
public class PhWeiboUserSchema implements ISchema {
    @Ch(ch = "id")
    public String pk;

    public String uid;

    @Ch(ch = "注册时间")
    public String publish_date;

    @Ch(ch = "更新时间")
    public String update_date;

    public String url;

    @Ch(ch = "简介")
    public String desc;

    @Ch(ch = "头像url")
    public String head_url;

    @Ch(ch = "用户名")
    public String name;

    @Ch(ch = "出生日期")
    public String birthdate;

    @Ch(ch = "出生年份")
    public String birthyear;

    @Ch(ch = "星座")
    public String constellation;

    @Ch(ch = "性别")
    public String gender;

    @Ch(ch = "省份")
    public String province;

    @Ch(ch = "城市")
    public String city;

    @Ch(ch = "城市级别")
    public String city_level;

    @Ch(ch = "粉丝级别")
    public String fans_level;

    @Type(type = Long.class)
    @Ch(ch = "微博数")
    public String wb_cnt;

    @Type(type = Long.class)
    @Ch(ch = "收藏数")
    public String fav_cnt;

    @Ch(ch = "粉丝数")
    public String fans_cnt;

    @Ch(ch = "关注数")
    public String follow_cnt;

    @Ch(ch = "双向关注数")
    public String bi_follow_cnt;

    @Ch(ch = "微博用户类型", cmt = "-1普通用户, 0名人, 1政府, 2企业, 3媒体, 4校园, 5网站, 6应用, 7团体（机构）, 8待审企业, 200初级达人, 220中高级达人, 400已故V用户。")
    public String verified_type;

    @Ch(ch = "认证类型", cmt = "普通用户 = 0, 蓝v = 1, 黄v = 2, 微博达人 = 3")
    public String vtype;

    @Ch(ch = "用户类型", cmt = "-1:未知类型, 0:正常用户帐号, 1:机构帐号, 2:僵尸")
    public String user_type;

    @Ch(ch = "客户端")
    public String sources;

    @Ch(ch = "关键词")
    public String keywords;

    @Ch(ch = "主题")
    public String topics;

    @Ch(ch = "爱好标签")
    public String meta_group;

    @Ch(ch = "活跃度")
    public String activeness;

    @Ch(ch = "公司")
    public String company;

    @Ch(ch = "学校")
    public String school;

    @Ch(ch = "电影")
    public String movies;

    @Ch(ch = "表情")
    public String emojis;

    @Ch(ch = "书")
    public String books;

    @Ch(ch = "应用")
    public String apps;

    @Ch(ch = "组织")
    public String orgs;

    @Ch(ch = "品牌")
    public String brands;

    @Ch(ch = "戏剧")
    public String dramas;

    @Ch(ch = "零售商")
    public String retailer;

    @Ch(ch = "音乐")
    public String music;

    @Ch(ch = "人名")
    public String fnames;

    @Ch(ch = "地名")
    public String loc_names;

    @Ch(ch = "产品")
    public String products;

    @Ch(ch = "价格感知")
    public String price_label;

    @Ch(ch = "促销")
    public String sales_promotion;

    @Ch(ch = "电视剧")
    public String tv_shows;
}
