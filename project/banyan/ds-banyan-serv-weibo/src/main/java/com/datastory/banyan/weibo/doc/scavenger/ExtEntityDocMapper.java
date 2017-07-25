package com.datastory.banyan.weibo.doc.scavenger;

import com.datastory.banyan.doc.DocMapper;
import com.datatub.scavenger.model.ExtEntity;
import com.yeezhao.commons.util.Entity.Params;

import static com.datastory.banyan.utils.BanyanTypeUtil.merge;
import static com.datastory.banyan.utils.BanyanTypeUtil.safePut;

/**
 * com.datastory.banyan.weibo.doc.scavenger.ExtEntityDocMapper
 *
 * @author lhfcws
 * @since 16/12/4
 */

public class ExtEntityDocMapper extends DocMapper {
    private ExtEntity ext;

    public ExtEntityDocMapper(ExtEntity extEntity) {
        this.ext = extEntity;
    }

    @Override
    public String getString(String key) {
        return null;
    }

    @Override
    public Integer getInt(String key) {
        return null;
    }

    @Override
    public Params map() {
        Params user = new Params();

        safePut(user, "movies", merge(ext.getMovies()));
        safePut(user, "apps", merge(ext.getApps()));
        safePut(user, "fnames", merge(ext.getFnames()));
        safePut(user, "books", merge(ext.getBooks()));
        safePut(user, "brands", merge(ext.getBrands()));
        safePut(user, "dramas", merge(ext.getDramas()));
        safePut(user, "emojis", merge(ext.getEmojis()));
        safePut(user, "music", merge(ext.getMusics()));
        safePut(user, "sales_promotion", merge(ext.getSalePromotion()));
        safePut(user, "price_label", merge(ext.getPriceLabel()));
        safePut(user, "orgs", merge(ext.getOrgs()));
        safePut(user, "products", merge(ext.getProducts()));
        safePut(user, "loc_names", merge(ext.getLocNames()));
        safePut(user, "tv_shows", merge(ext.getTvShows()));
        safePut(user, "retailer", merge(ext.getRetailers()));

        return user;
    }
}
