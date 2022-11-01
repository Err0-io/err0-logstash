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
        // the token/url is in the starter database, in the junit tests (with UAT data).
        configValues.put(Err0Output.URL.name(), "https://open-source.dev.err0.io:8443/");
        configValues.put(Err0Output.TOKEN.name(), "2.03_x09afBhtPu4P-wE4n7t9Vo7wAxToRNCuTIzlct9-uwn3LQC3woLkjwRMZKKvVLbag7cikzaR17lqqPm6Ueb8KCJcjnppedb7RYCGx4DqrQ2JIcua4mA.VnXTNen36BB2H_Ov8PBJFIQiO_y2nJHIXse-BEoBCLU");

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
