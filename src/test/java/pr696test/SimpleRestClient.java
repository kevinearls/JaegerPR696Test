package pr696test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static pr696test.PR696Test.SERVICE_NAME;

public class SimpleRestClient {
    private static Map<String, String> evs = System.getenv();
    private static final Integer JAEGER_FLUSH_INTERVAL = new Integer(evs.getOrDefault("JAEGER_FLUSH_INTERVAL", "1000"));
    private static final String JAEGER_QUERY_HOST = evs.getOrDefault("JAEGER_QUERY_HOST", "localhost");
    private static final Integer JAEGER_QUERY_SERVICE_PORT = new Integer(evs.getOrDefault("JAEGER_QUERY_SERVICE_PORT", "16686"));

    // Limit for the number of retries when getting traces
    private static final Integer RETRY_LIMIT = 10;

    private ObjectMapper jsonObjectMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(SimpleRestClient.class);

    private List<JsonNode> getTraces(String parameters) {
        List<JsonNode> traces = new ArrayList<>();
        Client client = ClientBuilder.newClient();
        String targetUrl = "http://" + JAEGER_QUERY_HOST + ":" + JAEGER_QUERY_SERVICE_PORT + "/api/traces?service=" + SERVICE_NAME;
        if (parameters != null && !parameters.trim().isEmpty()) {
            targetUrl = targetUrl + "&" + parameters;
        }

        logger.info("GETTING TRACES: " + targetUrl);
        try  {
            WebTarget target = client.target(targetUrl);
            Invocation.Builder builder = target.request();
            builder.accept(MediaType.APPLICATION_JSON);
            String result = builder.get(String.class);

            JsonNode jsonPayload = jsonObjectMapper.readTree(result);
            JsonNode data = jsonPayload.get("data");
            Iterator<JsonNode> traceIterator = data.iterator();
            while (traceIterator.hasNext()) {
                traces.add(traceIterator.next());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            client.close();
        }

        return traces;
    }


    /**
     *
     * @param parameters Parameter string to be appended to REST call
     * @param expectedTraceCount expected number of traces
     * @return a List of traces returned from the Jaeger Query API
     */
    public List<JsonNode> getTraces(String parameters, int expectedTraceCount) {
        List<JsonNode> traces = new ArrayList<>();
        int iterations = 0;

        // Retry for up to RETRY_LIMIT seconds to get the expected number of traces
        while (iterations < RETRY_LIMIT && traces.size() < expectedTraceCount) {
            iterations++;
            traces = getTraces(parameters);
            if (traces.size() >= expectedTraceCount) {
                return traces;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.warn("Sleep was interrupted", e);
            }
        }
        return traces;
    }

    /**
     * Return all of the traces created since the start time given.  NOTE: The Jaeger Rest API
     * requires a time in microseconds.
     *
     * @param testStartTime time the test started
     * @return A List of Traces created after the time specified.
     */
    public List<JsonNode> getTracesSinceTestStart(Instant testStartTime, int expectedTraceCount) {
        long startTime = TimeUnit.MILLISECONDS.toMicros(testStartTime.toEpochMilli());
        List<JsonNode> traces = getTraces("start=" + startTime+ "&limit=" + expectedTraceCount, expectedTraceCount);
        return traces;
    }

    /**
     * Return all of the traces created between the start and end times given.  NOTE: The Jaeger Rest API requires times
     * in microseconds.
     *
     * @param testStartTime start time
     * @param testEndTime end time
     * @return A List of traces created between the times specified.
     */
    public List<JsonNode> getTracesBetween(Instant testStartTime, Instant testEndTime, int expectedTraceCount) {
        long startTime = TimeUnit.MILLISECONDS.toMicros(testStartTime.toEpochMilli());
        long endTime = TimeUnit.MILLISECONDS.toMicros(testEndTime.toEpochMilli());
        String parameters = "start=" + startTime + "&end=" + endTime + "&limit=" + expectedTraceCount;
        List<JsonNode> traces = getTraces(parameters, expectedTraceCount);
        return traces;
    }


    /**
     * Make sure spans are flushed before trying to retrieve them
     */
    public void waitForFlush() {
        try {
            Thread.sleep(JAEGER_FLUSH_INTERVAL);
        } catch (InterruptedException e) {
            logger.warn("Sleep interrupted", e);
        }
    }
}

