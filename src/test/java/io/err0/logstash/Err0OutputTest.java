package io.err0.logstash;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Event;
import org.junit.Test;
import org.logstash.plugins.ConfigurationImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Err0OutputTest {

    @Test
    public void testJavaOutputExample() {
        Map<String, Object> configValues = new HashMap<>();
        configValues.put(Err0Output.URL.name(), "https://open-source.dev.err0.io:8443/");
        configValues.put(Err0Output.TOKEN.name(), "2.MFRF-aOr9f8BS9oMHGatWXfIYGY_m5IPWpMQPccLxXZqSSGGx8JJgsJOfYwLtelzgZu63kZIpZGpnQW5sw_wAQbGQvHQxAHz421yrEZuk0zywZm_Gt4kVw.SVFJddGR3KTR_nAA_bYivKrIfh0GbvsXLxY0x2qOlug");

        Configuration config = new ConfigurationImpl(configValues);
        Err0Output output = new Err0Output("test-id", config, null);

        String sourceField = "message";
        int eventCount = 40;
        Collection<Event> events = new ArrayList<>();
        for (int k = 0; k < eventCount; k++) {
            for (int i = 0, l = (int)(Math.random() * 10.0); i < l; i++) {
                Event e = new org.logstash.Event();
                e.setField(sourceField, "[KUB-" + k + "] example log line");
                events.add(e);
            }
        }

        output.output(events);
    }
}
