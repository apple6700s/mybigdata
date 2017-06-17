package com.datastory.banyan.schema;

import com.datastory.banyan.schema.annotation.Ch;
import com.datastory.banyan.schema.annotation.Inherit;

/**
 * com.datastory.banyan.schema.PhWechatSchema
 *
 * @author lhfcws
 * @since 2017/5/19
 */
@Inherit(include = "*", exclude = "cat_id,site_id")
public class PhWechatSchema extends PhCommonSchema {

    @Ch(ch = "点赞数")
    public String like_cnt;

    @Ch(ch = "阅读数")
    public String view_cnt;

    @Ch(ch = "是否原创")
    public String is_original;

    @Ch(ch = "缩略图")
    public String thumbnail;

    @Ch(ch = "简介")
    public String brief;

    public String other_data;

    @Ch(ch = "源链接")
    public String src_url;

    public String article_id;

    public String biz;

    public String sn;

    public String idx;

    public String mid;

    @Ch(ch = "微信公众号id")
    public String wxid;

    @Ch(ch = "本文作者")
    public String wx_author;
}
