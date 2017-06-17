package com.datastory.banyan.schema;


import com.datastory.banyan.schema.annotation.Ch;
import com.datastory.banyan.schema.annotation.Type;

/**
 * com.datatub.banyan.commons.schema.PhCommonSchema
 *
 * @author lhfcws
 * @since 2017/5/19
 */
public class PhCommonSchema implements ISchema {
    @Ch(ch = "id")
    public String pk;

    @Ch(ch="内容")
    public String content;

    @Ch(ch="标题")
    public String title;

    @Ch(ch="作者")
    public String author;

    @Ch(ch="发布时间")
    public String publish_date;

    @Ch(ch="更新时间")
    public String update_date;

    public String url;

    @Ch(ch="情感")
    public String sentiment;

    @Ch(ch="关键词")
    public String keywords;

    @Ch(ch="是否广告", cmt="1 是, 0 否")
    public String is_ad;

    @Ch(ch="类别id")
    @Type(type = Integer.class)
    public String cat_id;

    @Ch(ch="站点id")
    public String site_id;
}
