package com.dt.mig.sync.es;

import com.dt.mig.sync.base.MigSyncConsts;

/**
 * com.dt.mig.sync.es.MigEsWeiboUserWriter
 *
 * @author lhfcws
 * @since 2017/2/23
 */
public class MigEsNewsPostWriter extends CommonWriter {
    public MigEsNewsPostWriter() {
        super(MigSyncConsts.ES_NEWS_FORUM_WRITER_INDEX, MigSyncConsts.ES_NEWS_FORUM_PARENT_WRITE_TYPE);
    }

    private static volatile MigEsNewsPostWriter _singleton = null;

    public static MigEsNewsPostWriter getInstance() {
        if (_singleton == null) {
            synchronized (MigEsNewsPostWriter.class) {
                if (_singleton == null) {
                    _singleton = new MigEsNewsPostWriter();
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
