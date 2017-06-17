package com.datastory.banyan.schema;

import com.datastory.banyan.schema.annotation.Ch;

/**
 * com.datastory.banyan.schema.PhNewsPostSchema
 *
 * @author lhfcws
 * @since 2017/5/19
 */
public class PhNewsPostSchema extends PhCommonSchema {
    @Ch(ch="是否主贴")
    public String is_main_post;

    @Ch(ch = "站点名字")
    public String site_name;

    @Ch(ch = "新闻来源")
    public String source;

    @Ch(ch = "是否置顶")
    public String is_top;

    @Ch(ch = "是否精华帖")
    public String is_digest;

    @Ch(ch = "是否火贴")
    public String is_hot;

    @Ch(ch = "是否推荐")
    public String is_recom;

    @Ch(ch = "是否水贴")
    public String is_robot;

    @Ch(ch = "阅读数")
    public String view_cnt;

    @Ch(ch = "评论数")
    public String review_cnt;
}
