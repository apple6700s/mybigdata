package com.datastory.commons3.es.lucene_writer;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Constants;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.deletionpolicy.KeepOnlyLastDeletionPolicy;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityLookupService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.IndexStoreModule;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.indices.mapper.MapperRegistry;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * com.datastory.commons3.es.lucene_writer.LocalIndexService
 * <p>
 * 将客户端发过来的IndexRequest转化为Engine中处理的Index对象
 * 核心需知道MapperService怎么创建的
 * <p>
 * 一个IndexRequest2Index对象只能服务指定的单个索引 ，因为这决定了唯一的mapping。
 *
 * @author lhfcws
 * @since 2017/4/28
 */
public class LocalIndexService {
    /**
     * 管理mapping的服务
     */
    MapperService mapperService;
    AnalysisService analysisService;
    SimilarityLookupService similarityLookupService;
    IndexSettingsService indexSettingsService;
    CodecService codecService;
    SimilarityService similarityService;
    IndexDeletionPolicy indexDeletionPolicy;

    Settings settings;

    String indexName;
    Index index;
    ShardId shardId;
    /**
     * 绝对路径
     */
    String localStoreDirectory = null;

    public LocalIndexService() {
    }

    void init(String indexName, int shardIdNum, Map<String, String> configs) throws IOException {
        this.indexName = indexName;
        this.index = new Index(indexName);
        this.shardId = new ShardId(indexName, shardIdNum);

        this.settings = DefaultSettings.create();
        if (configs != null)
            this.settings = Settings.builder().put(this.settings).put(configs).build();
        else
            this.settings = Settings.builder().put(this.settings).build();

        /**
         * 其实会连带创建其他几个service
         */
        this.mapperService = newMapperService();
        this.codecService = new CodecService(index, this.indexSettingsService, mapperService);
        this.similarityService = createSimilarityService();
        this.indexDeletionPolicy = createIndexDeletionPolicy();
    }

    public String getLocalStoreDirectory() {
        return localStoreDirectory;
    }

    public void setLocalStoreDirectory(String localStoreDirectory) {
        this.localStoreDirectory = localStoreDirectory;
    }

    public int getShardIdNum() {
        return shardId.getId();
    }

    public MapperService getMapperService() {
        return mapperService;
    }

    public Engine.Index toIndex(IndexRequest request) {
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.PRIMARY, request.source()).index(request.index()).type(request.type()).id(request.id())
                .routing(request.routing()).parent(request.parent()).timestamp(request.timestamp()).ttl(request.ttl());
        return prepareIndex(docMapper(sourceToParse.type()), sourceToParse, request.version(), request.versionType(), Engine.Operation.Origin.PRIMARY, false);
    }

    public Codec getCodec() {
        if (codecService == null) {
            codecService = new CodecService(index, this.indexSettingsService, mapperService);
        }
        String codecName = settings.get(EngineConfig.INDEX_CODEC_SETTING, "default");
        return codecService.codec(codecName);
    }

    public void updateSettings(Settings settings) {
        this.settings = Settings.builder()
                .put(this.settings)
                .put(settings)
                .build();
    }

    public Directory getLuceneDirectory() throws IOException {
        int nodeOrdinal = settings.getAsInt("node.ordinal", 0);

        if (localStoreDirectory == null) {
            String dataPath = settings.get("path.data");
            new File(dataPath).mkdirs();

            String cluster = settings.get("cluster.name");

            Path location = Paths.get(dataPath + "/" + cluster + "/nodes/" + nodeOrdinal + "/indices/" + index.getName() + "/" + shardId.getId() + "/index");
            if (settings.get("local.path.data") != null) {
                location = Paths.get(settings.get("local.path.data"));
            }
            LockFactory lockFactory = buildLockFactory(settings);

            final String storeType = settings.get(IndexStoreModule.STORE_TYPE, IndexStoreModule.Type.DEFAULT.getSettingsKey());
            if (IndexStoreModule.Type.FS.match(storeType) || IndexStoreModule.Type.DEFAULT.match(storeType)) {
                final FSDirectory open = FSDirectory.open(location, lockFactory); // use lucene defaults
                if (open instanceof MMapDirectory && !Constants.WINDOWS) {
                    return newDefaultDir(location, (MMapDirectory) open, lockFactory);
                }
                return open;
            } else if (IndexStoreModule.Type.SIMPLEFS.match(storeType)) {
                return new SimpleFSDirectory(location, lockFactory);
            } else if (IndexStoreModule.Type.NIOFS.match(storeType)) {
                return new NIOFSDirectory(location, lockFactory);
            } else if (IndexStoreModule.Type.MMAPFS.match(storeType)) {
                return new MMapDirectory(location, lockFactory);
            }
            throw new IllegalArgumentException("No directory found for type [" + storeType + "]");
        } else {
            Path location = Paths.get(localStoreDirectory);
            LockFactory lockFactory = buildLockFactory(settings);
            return FSDirectory.open(location, lockFactory);
        }
    }


    public MapperService newMapperService() throws IOException {
        return newMapperService(DefaultSettings.create(), new IndicesModule());
    }

    public MapperService newMapperService(Settings indexSettings, IndicesModule indicesModule) throws IOException {
        if (settings == null) {
            Settings.Builder settingsBuilder = Settings.builder().put(indexSettings);
            if (indexSettings.get(IndexMetaData.SETTING_VERSION_CREATED) == null) {
                settingsBuilder.put(IndexMetaData.SETTING_VERSION_CREATED, DefaultVersion.get());
            }
            settings = settingsBuilder.build();
        }

        MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();
        analysisService = newAnalysisService(settings);
        similarityLookupService = newSimilarityLookupService(settings);
        MapperService ms = new MapperService(new Index(indexName),
                settings,
                analysisService,
                similarityLookupService,
                null,
                mapperRegistry);

        requestMapping(ms);

        return ms;
    }

    private void requestMapping(MapperService ms) throws IOException {
        String host = settings.get("network.publish_host", "localhost");
        String port = settings.get("http.port", "9200");
        String http = "http://";
        if (settings.getAsBoolean("ssl.enable", false)) {
            http = "https://";
        }

        String url = http + host + ":" + port + "/" + this.indexName + "/_mapping";
        System.out.println("[RequestMapping] " + url);

        Request request = new Request.Builder().url(url).get().build();

        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(10, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build();

        Response response = null;
        String body = null;
        int retry = 3;
        Exception requestException = null;
        while (retry-- >= 0) {
            try {
                response = client.newCall(request).execute();
                body = response.body().string();
                requestException = null;
            } catch (Exception e) {
                if (response != null)
                    response.close();
                retry--;
                requestException = e;
            } finally {
                if (response != null) {
                    response.body().close();
                    response.close();
                }
            }
        }

        if (requestException != null)
            throw new IOException("Http ERROR: " + requestException);

        // parse
        JSONObject mapping = JSONObject.parseObject(body);
        mapping = mapping.getJSONObject(this.indexName).getJSONObject("mappings");

        for (String type : mapping.keySet()) {
            JSONObject typeMapping = mapping.getJSONObject(type);
            String json = typeMapping.toJSONString();
            ms.merge(type, new CompressedXContent(new BytesArray(json)), MapperService.MergeReason.MAPPING_RECOVERY, true);
        }
    }

    /**
     * Copy from org.elasticsearch.index.shard.IndexShard #docMapper
     *
     * @param type
     * @return
     */
    private DocumentMapperForType docMapper(String type) {
        return mapperService.documentMapperWithAutoCreate(type);
    }

    private LockFactory buildLockFactory(Settings indexSettings) {
        String fsLock = indexSettings.get("index.store.fs.lock", indexSettings.get("index.store.fs.fs_lock", "native"));
        LockFactory lockFactory;
        if (fsLock.equals("native")) {
            lockFactory = NativeFSLockFactory.INSTANCE;
        } else if (fsLock.equals("simple")) {
            lockFactory = SimpleFSLockFactory.INSTANCE;
        } else {
            throw new IllegalArgumentException("unrecognized fs_lock \"" + fsLock + "\": must be native or simple");
        }
        return lockFactory;
    }

    private static final Set<String> PRIMARY_EXTENSIONS = Collections.unmodifiableSet(Sets.newHashSet("nvd", "dvd", "tim"));

    private Directory newDefaultDir(Path location, final MMapDirectory mmapDir, LockFactory lockFactory) throws IOException {
        return new FileSwitchDirectory(PRIMARY_EXTENSIONS, mmapDir, new NIOFSDirectory(location, lockFactory), true) {
            @Override
            public String[] listAll() throws IOException {
                // Avoid doing listAll twice:
                return mmapDir.listAll();
            }
        };
    }


    private AnalysisService newAnalysisService(Settings indexSettings) {
        Injector parentInjector = new ModulesBuilder().add(new SettingsModule(indexSettings), new EnvironmentModule(new Environment(indexSettings))).createInjector();
        Index index = new Index(indexName);

        IndicesAnalysisService indicesAnalysisService = parentInjector.getInstance(IndicesAnalysisService.class);
        AnalysisModule analysisModule = new AnalysisModule(indexSettings, indicesAnalysisService);

        for (AnalysisModule.AnalysisBinderProcessor processor : CustomizedAnalyzers.getCustomizedAnalyzers()) {
            analysisModule.addProcessor(processor);
        }

        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(index, indexSettings),
                new IndexNameModule(index),
                analysisModule
        ).createChildInjector(parentInjector);
        indexSettingsService = injector.getInstance(IndexSettingsService.class);
        return injector.getInstance(AnalysisService.class);
    }

    private SimilarityLookupService newSimilarityLookupService(Settings indexSettings) {
        return new SimilarityLookupService(new Index(indexName), indexSettings);
    }

    /**
     * Copy from org.elasticsearch.index.shard.IndexShard #prepareIndex
     *
     * @param docMapper
     * @param source
     * @param version
     * @param versionType
     * @param origin
     * @param canHaveDuplicates
     * @return
     */
    private Engine.Index prepareIndex(DocumentMapperForType docMapper, SourceToParse source, long version, VersionType versionType, Engine
            .Operation.Origin origin, boolean canHaveDuplicates) {
        long startTime = System.nanoTime();
        ParsedDocument doc = docMapper.getDocumentMapper().parse(source);
        if (docMapper.getMapping() != null) {
            doc.addDynamicMappingsUpdate(docMapper.getMapping());
        }
        return new Engine.Index(docMapper.getDocumentMapper().uidMapper().term(doc.uid().stringValue()), doc, version, versionType,
                origin, startTime, canHaveDuplicates);
    }

    private SimilarityService createSimilarityService() {
        return new SimilarityService(
                index, indexSettingsService, similarityLookupService, mapperService
        );
    }

    private IndexDeletionPolicy createIndexDeletionPolicy() {
        return new KeepOnlyLastDeletionPolicy(shardId, settings);
    }
}
