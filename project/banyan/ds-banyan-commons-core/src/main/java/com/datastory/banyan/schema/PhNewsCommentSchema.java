package com.datastory.banyan.schema;

import com.datastory.banyan.schema.annotation.Ch;

/**
 * com.datastory.banyan.schema.PhNewsCommentSchema
 *
 * @author lhfcws
 * @since 2017/5/19
 */
public class PhNewsCommentSchema extends PhCommonSchema {
    @Ch(ch="是否主贴")
    public String is_main_post;

    @Ch(ch = "站点名字")
    public String site_name;

    @Ch(ch = "新闻来源")
    public String source;

    @Ch(ch = "是否水贴")
    public String is_robot;

    @Ch(ch = "主贴id")
    public String parent_post_id;
}
