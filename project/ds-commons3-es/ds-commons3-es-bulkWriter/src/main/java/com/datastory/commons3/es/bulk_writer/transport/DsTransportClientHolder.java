package com.datastory.commons3.es.bulk_writer.transport;

import org.apache.log4j.Logger;
import org.elasticsearch.common.settings.Settings;

public class DsTransportClientHolder {

    private static final Logger LOG = Logger.getLogger(DsTransportClientHolder.class);

    private static volatile DsTransportClient INTANCE;

    public static void initialize(Settings settings) {
        if (INTANCE == null) {
            synchronized (DsTransportClientHolder.class) {
                if (INTANCE == null) {
                    INTANCE = DsTransportClient.builder().settings(settings).build();
                    Runtime.getRuntime().addShutdownHook(new Thread() {
                        @Override
                        public void run() {
                            LOG.info("close DsTransportClient in shutdown hook");
                            INTANCE.close();
                        }
                    });
                } else {
                    throw new RuntimeException("already initialized");
                }
            }
        }
    }

    public static void initializeIfNot(Settings settings) {
        if (INTANCE == null) {
            synchronized (DsTransportClientHolder.class) {
                if (INTANCE == null) {
                    INTANCE = DsTransportClient.builder().settings(settings).build();
                    Runtime.getRuntime().addShutdownHook(new Thread() {
                        @Override
                        public void run() {
                            LOG.info("close DsTransportClient in shutdown hook");
                            INTANCE.close();
                        }
                    });
                }
            }
        }
    }

    public static DsTransportClient get() {
        if (INTANCE == null) {
            synchronized (DsTransportClientHolder.class) {
                if (INTANCE == null) {
                    throw new RuntimeException("invoke initialize() first");
                }
            }
        }
        return INTANCE;
    }
}
