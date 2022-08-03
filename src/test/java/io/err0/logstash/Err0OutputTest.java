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
        configValues.put(Err0Output.TOKEN.name(), "2.PXQvjFDr1iAjpHWnC1Vb-mCMvovZVV85KfRxfMssJPUPYwWoMt9afyGDZMoeLd0qLI85Z_YTaNWo60nBE2k6x_TaLMIRBMtAfqjZEt18yiF2POjIPbpWVg.nWfWYUw3TDeadEOiE3hG5WCM2VKSk6fMJ8xa5AEiwrI");

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
