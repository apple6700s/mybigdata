package com.datastory.banyan.schema;

import com.datastory.banyan.schema.annotation.Ch;

/**
 * com.datastory.banyan.schema.PhWechatMpSchema
 *
 * @author lhfcws
 * @since 2017/5/27
 */
public class PhWechatMpSchema implements ISchema {
    public String biz;

    @Ch(ch = "公众号名")
    public String name;

    @Ch(ch = "描述")
    public String desc;

    @Ch(ch = "更新时间")
    public String update_date;

    @Ch(ch = "粉丝数")
    public String fans_cnt;

    @Ch(ch = "认证状态", cmt = "-1 未认证 ，0 审核中， 1 已认证 ，2 微信认证，3 微博认证，4 手机认证， 5 邮箱认证, 6 实名认证，7 教育认证，8 公司认证")
    public String verify_status;

    @Ch(ch = "微信id")
    public String wxid;

    @Ch(cmt = "搜狗微信openid")
    public String open_id;
}
