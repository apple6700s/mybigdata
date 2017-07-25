package com.datastory.commons3.es.lucene_writer;

import com.google.common.reflect.TypeToken;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.en.WordsENAnalysisBinderProcessor;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;

/**
 * com.datastory.commons3.es.lucene_writer.DefaultSettings
 *
 * @author lhfcws
 * @since 2017/4/28
 */
public class DefaultSettings {
    static String confFile = "elasticsearch.yml";
    static String pathHome = null;

    static {
        // for datastory wordsEN
        CustomizedAnalyzers.add(new WordsENAnalysisBinderProcessor());
    }

    public static void initEmptyPathHome() {
        if (!System.getenv().containsKey("ES_HOME") && !System.getProperties().containsKey("path.home"))
            System.getProperties().put("path.home", "/tmp");
    }

    public static Settings create(InputStream in) {
        Map<? extends Object, ? extends Object> config = null;
        try {
            config = loadElasticSearchYml(in);
            return fromMap(config);
        } catch (Exception ignore) {
            return null;
        }
    }

    public static Settings create() {
        Map<? extends Object, ? extends Object> config = null;
        try {
            config = loadElasticSearchYml();
        } catch (Exception ignore) {}

        if (pathHome == null && config != null)
            pathHome = (String) config.get("path.home");

        if (pathHome == null && System.getProperties().containsKey("path.home"))
            pathHome = System.getProperty("path.home");

        if (pathHome == null) {
            pathHome = System.getenv("ES_HOME");
            if (pathHome == null)
                throw new RuntimeException("path.home is not set");
        }

        if (config == null)
            config = loadElasticSearchYml();

        return fromMap(config);
    }

    public static Settings fromMap(Map<? extends Object, ? extends Object> config) {
        Settings.Builder builder = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, DefaultVersion.get());
        for (Map.Entry e : config.entrySet()) {
            builder.put(e.getKey(), e.getValue());
        }
        if (pathHome != null) {
            builder.put("path.home", Paths.get(URI.create("file://" + pathHome)));
        }

        if (System.getProperties().containsKey("es.mapping.configs")) {
            String json = System.getProperty("es.mapping.configs");
            if (json != null) {
                Map<String, Object> customConfigs = FastJsonSerializer.deserialize(json, new TypeToken<Map<String, Object>>() {
                }.getType());
                for (Map.Entry<String, Object> e : customConfigs.entrySet()) {
                    builder.put(e.getKey(), e.getValue());
                }
            }
        }

        return builder.build();
    }

    /**
     * Loads the configuration.
     */
    public static Map loadElasticSearchYml() {
        Yaml yaml = new Yaml();

        try {
            return (Map<?, ?>) yaml.load(loadResource(confFile));
        } catch (Exception e) {
            if (System.getProperties().containsKey("path.home"))
                pathHome = System.getProperty("path.home");
            try {
                return (Map<?, ?>) yaml.load(new FileInputStream(pathHome + "/config/" + confFile));
            } catch (Exception e1) {
                throw new RuntimeException("elasticsearch.yml is not provided: " + e.getMessage());
            }
        }
    }

    public static Map loadElasticSearchYml(InputStream in) {
        if (in != null) {
            Yaml yaml = new Yaml();
            return (Map<?, ?>) yaml.load(in);
        } else
            return loadElasticSearchYml();
    }

    public static InputStream loadResource(String name) throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = DefaultSettings.class.getClassLoader();
        }
        URL url = classLoader.getResource(name);
        if (url == null)
            throw new FileNotFoundException(name);
        else
            return url.openStream();
    }

}
