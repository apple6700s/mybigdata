package com.datastory.banyan.schema;


import com.datastory.banyan.schema.annotation.Ch;
import com.datastory.banyan.schema.annotation.Inherit;
import com.datastory.banyan.schema.annotation.Type;

/**
 * com.datatub.banyan.commons.schema.PhWeiboSchema
 *
 * @author lhfcws
 * @since 2017/5/19
 */
@Inherit(include = "*", exclude = "title,author,cat_id,site_id")
public class PhWeiboSchema extends PhCommonSchema {

    public String mid;

    public String uid;

    @Ch(ch = "原创内容")
    public String self_content;

    @Ch(ch = "源内容")
    public String src_content;

    @Ch(ch = "源微博id")
    public String src_mid;

    @Ch(ch = "源微博用户id")
    public String src_uid;

    @Ch(ch = "转发微博id")
    public String rt_mid;

    @Ch(ch = "微博类型", cmt = "－1:其他, 0:原帖, 1:第一层转发不带文字, 2:第一层转发带文字, 3:多层转发")
    public String msg_type;

    @Ch(ch = "转发数")
    @Type(type = Long.class)
    public String reposts_cnt;

    @Ch(ch = "评论数")
    @Type(type = Long.class)
    public String comments_cnt;

    @Ch(ch = "点赞数")
    @Type(type = Long.class)
    public String attitudes_cnt;

    @Ch(ch = "发布终端")
    public String source;

    @Ch(ch = "话题")
    public String topics;

    @Ch(ch = "表情")
    public String emoji;

    @Ch(ch = "提及")
    public String mention;
}
