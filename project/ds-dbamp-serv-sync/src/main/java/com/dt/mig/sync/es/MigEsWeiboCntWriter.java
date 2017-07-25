package com.dt.mig.sync.es;

import com.dt.mig.sync.base.MigSyncConsts;

/**
 * com.dt.mig.sync.es.MigEsWeiboCntWriter
 *
 * @author lhfcws
 * @since 2017/2/23
 */
public class MigEsWeiboCntWriter extends CommonWriter {
    public MigEsWeiboCntWriter() {
        super(MigSyncConsts.ES_WEIBO_WRITER_INDEX, MigSyncConsts.ES_WEIBO_CHILD_WRITE_TYPE);
    }

    public MigEsWeiboCntWriter(int num) {
        super(MigSyncConsts.ES_WEIBO_WRITER_INDEX, MigSyncConsts.ES_WEIBO_CHILD_WRITE_TYPE, num);
    }

    private static volatile MigEsWeiboCntWriter _singleton = null;

    public static MigEsWeiboCntWriter getInstance() {
        if (_singleton == null) {
            synchronized (MigEsWeiboCntWriter.class) {
                if (_singleton == null) {
                    _singleton = new MigEsWeiboCntWriter(5000);
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
