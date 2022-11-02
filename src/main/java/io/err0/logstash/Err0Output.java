package io.err0.logstash;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.Output;
import co.elastic.logstash.api.PluginConfigSpec;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// class name must match plugin name
@LogstashPlugin(name = "err0_output")
public class Err0Output implements Output {

    public static class Err0Log {
        public Err0Log(final String error_code, final long ts, final String message, final JsonObject metadata) {
            this.error_code = error_code;
            this.ts = ts;
            this.message = message;
            this.metadata = metadata;
        }
        public final String error_code;
        public final long ts;
        public final String message;
        public final JsonObject metadata;
    }

    public static final PluginConfigSpec<String> URL = PluginConfigSpec.stringSetting("url", "");
    public static final PluginConfigSpec<String> TOKEN = PluginConfigSpec.stringSetting("token", "");

    private final String id;
    private PrintStream printer;
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile boolean stopped = false;

    // all plugins must provide a constructor that accepts id, Configuration, and Context
    public Err0Output(final String id, final Configuration configuration, final Context context) {
        // constructors should validate configuration options
        this.id = id;
        String url = configuration.get(URL);
        try {
            this.url = new URL(url + "~/api/bulk-log");
        }
        catch (MalformedURLException ex) {
            this.url = null;
        }
        this.token = configuration.get(TOKEN);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                //System.err.println("shutdown hook");
                stopped = true;
                while (pollQueue()) {}
                Err0Http.shutdown();
                done.countDown();
            }
        }));
        this.thread.setDaemon(true);
        this.thread.start();
    }

    private URL url;
    private static Pattern pattern = Pattern.compile("\\[([A-Z][A-Z0-9]*-[0-9]+)\\]");
    private String token;

    private static final ConcurrentLinkedQueue<Err0Log> queue = new ConcurrentLinkedQueue<>();

    private boolean pollQueue() {
        try {
            ArrayList<Err0Log> list = new ArrayList<>();
            Err0Log logEvent = null;
            do {
                logEvent = queue.poll();
                if (null != logEvent) {
                    list.add(logEvent);
                }
            } while (null != logEvent);
            if (list.size() > 0) {
                JsonObject bulkLog = new JsonObject();
                JsonArray logs = new JsonArray();
                bulkLog.add("logs", logs);
                for (Err0Log log : list) {
                    //System.err.println("ERR0\t" + log.error_code + "\t" + log.ts + "\t" + log.message + "\t" + log.metadata.toString());

                    JsonObject o = new JsonObject();
                    o.addProperty("error_code", log.error_code);
                    o.addProperty("ts", Long.toString(log.ts));
                    o.addProperty("msg", log.message);
                    o.add("metadata", log.metadata);

                    logs.add(o);
                }
                Err0Http.call(url, token, bulkLog);
                return true;
            }
        }
        catch (Throwable t) {
            // ignore
        }
        return false;
    }

    private final Thread thread = new Thread() {
        @Override
        public void run() {
            for (;;) {
                if (!Err0Http.canCall()) {
                    Thread.yield();
                } else {
                    boolean wasEmpty = pollQueue();
                    if (wasEmpty) {
                        if (stopped) {
                            done.countDown();
                            return; // exit thread here, after sending last log to err0server.
                        }
                        Thread.yield();
                    }
                }
            }
        }
    };

    @Override
    public void output(final Collection<Event> events) {
        Iterator<Event> z = events.iterator();
        while (z.hasNext() && !stopped) {
            Event event = z.next();
            String formattedMessage = event.toString();
            final Matcher matcher = pattern.matcher(formattedMessage);
            while (matcher.find()) {
                final String error_code = matcher.group(1);
                final long ts = event.getEventTimestamp().toEpochMilli();
                final JsonObject metadata = new JsonObject();
                final JsonObject logstashData = new JsonObject();
                final JsonObject logstashMetadata = new JsonObject();
                final Map<String, Object> eventData = event.getData();
                if (null != eventData) {
                    for (Map.Entry<String, Object> entry : eventData.entrySet()) {
                        logstashData.addProperty(entry.getKey(), entry.getValue().toString());
                    }
                }
                final Map<String, Object> eventMetadata = event.getMetadata();
                if (null != eventMetadata) {
                    for (Map.Entry<String, Object> entry : eventMetadata.entrySet()) {
                        logstashMetadata.addProperty(entry.getKey(), entry.getValue().toString());
                    }
                }
                final JsonObject logstash = new JsonObject();
                if (!logstashData.entrySet().isEmpty()) {
                    logstash.add("data", logstashData);
                }
                if (!logstashMetadata.entrySet().isEmpty()) {
                    logstash.add("metadata", logstashMetadata);
                }
                if (!logstash.entrySet().isEmpty()) {
                    metadata.add("logstash", logstash);
                }
                queue.add(new Err0Log(error_code, ts, formattedMessage, metadata));
            }
        }
    }

    @Override
    public void stop() {
        stopped = true;
    }

    @Override
    public void awaitStop() throws InterruptedException {
        done.await();
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        // should return a list of all configuration options for this plugin
        ArrayList<PluginConfigSpec<?>> list = new ArrayList<>();
        list.add(URL);
        list.add(TOKEN);
        return list;
    }

    @Override
    public String getId() {
        return id;
    }
}
