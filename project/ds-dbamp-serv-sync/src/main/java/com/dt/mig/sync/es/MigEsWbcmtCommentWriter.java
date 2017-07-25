package com.dt.mig.sync.es;

import com.dt.mig.sync.base.MigSyncConsts;

/**
 * com.dt.mig.sync.es.MigEsWeiboCntWriter
 *
 * @author lhfcws
 * @since 2017/2/23
 */
public class MigEsWbcmtCommentWriter extends CommonWriter {
    public MigEsWbcmtCommentWriter() {
        super(MigSyncConsts.ES_WEIBO_COMMENT_WRITER_INDEX, MigSyncConsts.ES_WEIBO_COMMENT_CHILD_WRITE_TYPE);
    }

    public MigEsWbcmtCommentWriter(int num) {
        super(MigSyncConsts.ES_WEIBO_COMMENT_WRITER_INDEX, MigSyncConsts.ES_WEIBO_COMMENT_CHILD_WRITE_TYPE, num);
    }

    private static volatile MigEsWbcmtCommentWriter _singleton = null;

    public static MigEsWbcmtCommentWriter getInstance() {
        if (_singleton == null) {
            synchronized (MigEsWbcmtCommentWriter.class) {
                if (_singleton == null) {
                    _singleton = new MigEsWbcmtCommentWriter(5000);
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
