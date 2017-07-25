package com.dt.mig.sync.es;

import com.dt.mig.sync.base.MigSyncConsts;

/**
 * com.dt.mig.sync.es.MigEsWeiboCntWriter
 *
 * @author lhfcws
 * @since 2017/2/23
 */
public class MigEsWbcmtWeiboWriter extends CommonWriter {
    public MigEsWbcmtWeiboWriter() {
        super(MigSyncConsts.ES_WEIBO_COMMENT_WRITER_INDEX, MigSyncConsts.ES_WEIBO_COMMENT_PARENT_WRITE_TYPE);
        System.out.printf("index:%s , type:%s.\n", MigSyncConsts.ES_WEIBO_COMMENT_WRITER_INDEX, MigSyncConsts.ES_WEIBO_COMMENT_PARENT_WRITE_TYPE);
    }

    public MigEsWbcmtWeiboWriter(int num) {
        super(MigSyncConsts.ES_WEIBO_COMMENT_WRITER_INDEX, MigSyncConsts.ES_WEIBO_COMMENT_PARENT_WRITE_TYPE, num);
        System.out.printf("index:%s , type:%s.\n", MigSyncConsts.ES_WEIBO_COMMENT_WRITER_INDEX, MigSyncConsts.ES_WEIBO_COMMENT_PARENT_WRITE_TYPE);
    }

    private static volatile MigEsWbcmtWeiboWriter _singleton = null;

    public static MigEsWbcmtWeiboWriter getInstance() {
        if (_singleton == null) {
            synchronized (MigEsWbcmtWeiboWriter.class) {
                if (_singleton == null) {
                    _singleton = new MigEsWbcmtWeiboWriter(5000);
                    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                        @Override
                        public void run() {
                            _singleton.flush();
                            try {
                                Thread.sleep(2000);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            _singleton.close();
                        }
                    }));
                }
            }
        }
        return _singleton;
    }


}
