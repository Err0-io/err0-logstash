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
        configValues.put(Err0Output.URL.name(), "https://open-source-dev.err0.io:8443/");
        configValues.put(Err0Output.TOKEN.name(), "2.pHkDUXHbM-S-OkuePiq8NVdDN6ANtwwVPt1nzg5RkZw_oIcvWPyll61ulTtk5cdq-cqVtA3IKwjK7cAAYhDs0IosqkEoOjgMU7P4c5QdjwnusoPZqeND0w.SQ8R3sHTFQ5RJkrM-9bwTLRBkK1zNmeXhbj7W9KEcps");

        Configuration config = new ConfigurationImpl(configValues);
        Err0Output output = new Err0Output("test-id", config, null);

        String sourceField = "message";
        int eventCount = 5;
        Collection<Event> events = new ArrayList<>();
        for (int k = 0; k < eventCount; k++) {
            Event e = new org.logstash.Event();
            e.setField(sourceField, "[EG-" + k + "] example log line");
            events.add(e);
        }

        output.output(events);
    }
}
