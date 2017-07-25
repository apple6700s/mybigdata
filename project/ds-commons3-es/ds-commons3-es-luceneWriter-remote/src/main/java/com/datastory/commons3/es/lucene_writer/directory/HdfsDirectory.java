package com.datastory.commons3.es.lucene_writer.directory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.lucene.store.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;

/**
 * com.datastory.commons3.es.lucene_writer.directory.HdfsDirectory
 * For Hadoop 2, refer to https://github.com/booz-allen-hamilton/lucene-hdfs-directory/blob/master/src/main/java/com/bah/lucene/hdfs/HdfsDirectory.java
 *
 * @author lhfcws
 * @since 2017/5/3
 */
public class HdfsDirectory extends BaseDirectory {
    protected final Path _path;
    protected final FileSystem _fileSystem;

    public Configuration getConf() {
        return _fileSystem.getConf();
    }

    public static HdfsDirectory create(String path) throws IOException {
        Configuration configuration = new Configuration();
        configuration.addResource("core-site.xml");
        configuration.addResource("hdfs-site.xml");
        configuration.set("dfs.replication", "1");
        return new HdfsDirectory(configuration, new Path(path));
    }

    public static HdfsDirectory create(Configuration conf, String path) throws IOException {
        if (conf == null)
            return create(path);
        else
            return new HdfsDirectory(conf, new Path(path));
    }

    public HdfsDirectory(Configuration configuration, Path path) throws IOException {
        super(NoLockFactory.INSTANCE);
        this._path = path;
        _fileSystem = FileSystem.get(configuration);
        _fileSystem.mkdirs(path);
    }

    @Override
    public String toString() {
        return "HdfsDirectory path=[" + _path + "]";
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        if (fileExists(name)) {
            throw new IOException("File [" + name + "] already exists found.");
        }
        final FSDataOutputStream outputStream = openForOutput(name);
        return new OutputStreamIndexOutput(name, outputStream, 8096);
    }

    protected FSDataOutputStream openForOutput(String name) throws IOException {
        return _fileSystem.create(getPath(name));
    }

    /**
     * Not support read currently
     */
    @Override
    @Deprecated
    public IndexInput openInput(String name, IOContext context) throws IOException {
        if (!fileExists(name)) {
            throw new FileNotFoundException("File [" + name + "] not found.");
        }
        FSDataInputStream inputStream = openForInput(name);
        final long fileLength = fileLength(name);
        String desc = "[SimpleHdfsIndexInput] " + getPath(name).toString();
        return new SimpleHdfsIndexInput(desc, inputStream, fileLength);
    }

    protected FSDataInputStream openForInput(String name) throws IOException {
        return _fileSystem.open(getPath(name));
    }

    @Override
    public String[] listAll() throws IOException {
        FileStatus[] files = _fileSystem.listStatus(_path, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                try {
                    return _fileSystem.isFile(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        String[] result = new String[files.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = files[i].getPath().getName();
        }
        return result;
    }

    public boolean fileExists(String name) throws IOException {
        return exists(name);
    }

    protected boolean exists(String name) throws IOException {
        return _fileSystem.exists(getPath(name));
    }

    @Override
    public void deleteFile(String name) throws IOException {
        if (fileExists(name)) {
            delete(name);
        } else {
            throw new FileNotFoundException("File [" + name + "] not found");
        }
    }

    protected void delete(String name) throws IOException {
        _fileSystem.delete(getPath(name), true);
    }

    @Override
    public long fileLength(String name) throws IOException {
        return length(name);
    }

    protected long length(String name) throws IOException {
        return _fileSystem.getFileStatus(getPath(name)).getLen();
    }

    @Override
    public void sync(Collection<String> names) throws IOException {

    }

    @Override
    public void renameFile(String s, String s1) throws IOException {
        _fileSystem.rename(getPath(s), getPath(s1));
    }

    @Override
    public void close() throws IOException {

    }

    public Path getPath() {
        return _path;
    }

    private Path getPath(String name) {
        return new Path(_path, name);
    }

    public long getFileModified(String name) throws IOException {
        if (!fileExists(name)) {
            throw new FileNotFoundException("File [" + name + "] not found");
        }
        return fileModified(name);
    }

    protected long fileModified(String name) throws IOException {
        return _fileSystem.getFileStatus(getPath(name)).getModificationTime();
    }
}
