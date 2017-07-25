package com.dt.mig.sync.es;

import com.dt.mig.sync.base.MigSyncConsts;

/**
 * com.dt.mig.sync.es.MigEsWeiboUserWriter
 *
 * @author lhfcws
 * @since 2017/2/23
 */
public class MigEsWeiboUserWriter extends CommonWriter {
    public MigEsWeiboUserWriter() {
        super(MigSyncConsts.ES_WEIBO_WRITER_INDEX, MigSyncConsts.ES_WEIBO_PARENT_WRITE_TYPE);
    }

    private static volatile MigEsWeiboUserWriter _singleton = null;

    public static MigEsWeiboUserWriter getInstance() {
        if (_singleton == null) {
            synchronized (MigEsWeiboUserWriter.class) {
                if (_singleton == null) {
                    _singleton = new MigEsWeiboUserWriter();
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
