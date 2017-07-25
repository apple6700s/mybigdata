package com.datastory.commons3.es.lucene_writer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * com.datastory.commons3.es.lucene_writer.CopyDataTranslogWrite
 *
 * @author zhaozhen
 * @since 2017/6/16
 */
public class TranslogWriter {


    public boolean tryInitTranslog(String pathIndexData, String tryUuid) throws IOException {
        if (true) {
            long generation = 1;
            TranslogIniter translogIniter = new TranslogIniter();
            String translogUUid = tryUuid;
            File ckpPath = new File(translogIniter.getCkpPath(pathIndexData));
            File o = new File(ckpPath.getParent());
            o.mkdirs();
            FileOutputStream ckpOs = new FileOutputStream(ckpPath);
            File tlogPath = new File(translogIniter.getPath(pathIndexData));
            FileOutputStream os = new FileOutputStream(tlogPath);
            int tlogSize = translogIniter.write(os, translogUUid);
            translogIniter.writeInitCheckpoint(ckpOs, tlogSize, generation);
        }

        return true;
    }
}