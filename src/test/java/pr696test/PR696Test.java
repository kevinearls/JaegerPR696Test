package pr696test;

import com.fasterxml.jackson.databind.JsonNode;
import com.uber.jaeger.reporters.RemoteReporter;
import com.uber.jaeger.samplers.ProbabilisticSampler;
import com.uber.jaeger.samplers.Sampler;
import com.uber.jaeger.senders.HttpSender;
import com.uber.jaeger.senders.Sender;
import com.uber.jaeger.senders.UdpSender;
import io.opentracing.Span;
import io.opentracing.Tracer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PR696Test {
    private static final Logger logger = LoggerFactory.getLogger(PR696Test.class.getName());
    AtomicLong operationId = new AtomicLong(Instant.now().getEpochSecond());
    private static String OPERATION_NAME = "PR696;";
    private static Tracer tracer = null;
    private SimpleRestClient simpleRestClient = new SimpleRestClient();

    private static Map<String, String> envs = System.getenv();
    private static final Integer DELAY = Integer.valueOf(envs.getOrDefault("DELAY", "1")); // Delay between span creation
    private static final Integer GROUP_SIZE = Integer.valueOf(envs.getOrDefault("GROUP_SIZE", "20000"));
    private static final String JAEGER_AGENT_HOST = envs.getOrDefault("JAEGER_AGENT_HOST", "localhost");
    private static final Integer JAEGER_AGENT_PORT = Integer.valueOf(envs.getOrDefault("JAEGER_AGENT_PORT", "6831"));
    private static final String JAEGER_COLLECTOR_HOST  = envs.getOrDefault("JAEGER_COLLECTOR_HOST", "localhost");
    private static final String JAEGER_COLLECTOR_PORT = envs.getOrDefault("JAEGER_PORT_ZIPKIN_COLLECTOR", "14268");
    private static Integer JAEGER_FLUSH_INTERVAL = Integer.valueOf(envs.getOrDefault("JAEGER_FLUSH_INTERVAL", "1000"));
    private static final Integer RETRIEVALS = Integer.valueOf(envs.getOrDefault("RETRIEVALS", "5"));
    public static final String SERVICE_NAME  = envs.getOrDefault("SERVICE_NAME", "PR696");
    private static final Integer TRACE_COUNT = Integer.valueOf(envs.getOrDefault("TRACE_COUNT", "100000"));
    private static final String USE_COLLECTOR_OR_AGENT = envs.getOrDefault("USE_COLLECTOR_OR_AGENT", "collector");

    @Before
    public void setup() {
        Sender sender;
        if (USE_COLLECTOR_OR_AGENT.equals("collector")) {
            String httpEndpoint = "http://" + JAEGER_COLLECTOR_HOST + ":" + JAEGER_COLLECTOR_PORT + "/api/traces";
            logger.info("Using collector endpoint [" + httpEndpoint + "]");
            sender = new HttpSender(httpEndpoint);
        } else {
            sender = new UdpSender(JAEGER_AGENT_HOST, JAEGER_AGENT_PORT, 1024);
            logger.info("Using JAEGER agent on host " + JAEGER_AGENT_HOST + " port " + JAEGER_AGENT_PORT);
        }

        RemoteReporter remoteReporter = new RemoteReporter.Builder()
                .withSender(sender)
                .withFlushInterval(JAEGER_FLUSH_INTERVAL)
                .build();

        Sampler sampler = new ProbabilisticSampler(1.0);
        tracer = new com.uber.jaeger.Tracer.Builder(SERVICE_NAME)
                .withReporter(remoteReporter)
                .withSampler(sampler)
                .build();
    }

    @Test
    public void timeESTest() {
        timeElasticSearchRetrievals(TRACE_COUNT, GROUP_SIZE);
    }

    /**
     * Write 'traceCount' traces, then retrieve them 'RETRIEVALS' times in groups of 'groupSize'
     *
     * @param traceCount number of traces to create
     * @param groupSize number of traces to retrieve at a time
     */
    private void timeElasticSearchRetrievals(int traceCount, int groupSize)  {
        assertTrue("GroupSize must be <= traceCount!", groupSize <= traceCount);
        assertTrue("traceCount must be evenly divisible try groupSize", traceCount % groupSize == 0);
        logger.info("Running with " + traceCount + " traces, groupSize " + groupSize + " retrievals " + RETRIEVALS);
        Map<Instant, Instant> startEndPairs = new TreeMap<>();

        int groups = traceCount / groupSize;
        logger.info("Groups: " + groups);
        for (int i = 0; i < groups; i++) {
            Instant groupStartTime = Instant.now();
            sleep(1);
            for (int j = 0; j < groupSize; j++) {
                Span span = tracer.buildSpan(OPERATION_NAME)
                        .withTag("simple", true)
                        .start();
                span.finish();
                // TODO do I need a delay to be sure they all get written?
                sleep(DELAY);
            }
            Instant groupEndTime = Instant.now();
            startEndPairs.put(groupStartTime, groupEndTime);
            sleep(50);
        }

        // TODO add an assert here for all traces first?  That way we can be sure they are all in the db
        logger.info("Sleeping for  " + traceCount);
        sleep(traceCount);

        long totalDuration = 0;
        for (int i=0; i < RETRIEVALS; i++) {
            for (Instant groupStartTime : startEndPairs.keySet()) {
                Instant groupEndTime = startEndPairs.get(groupStartTime);
                List<JsonNode> traces;
                Instant retrievalStartTime = Instant.now();
                if (groups == 1) {
                    System.out.println("Getting traces staring at " + groupEndTime + " group " + groupSize);
                    traces = simpleRestClient.getTracesSinceTestStart(groupStartTime, groupSize);
                } else {
                    System.out.println("Getting traces between " + groupStartTime + " and " + groupEndTime + " " + " group " + groupSize);
                    traces = simpleRestClient.getTracesBetween(groupStartTime, groupEndTime, groupSize);
                }
                Instant retrievalEndTime = Instant.now();
                Duration testDuration = Duration.between(retrievalStartTime, retrievalEndTime);
                logger.info("This test lasted " + testDuration.toMillis());
                totalDuration += testDuration.toMillis();
                assertEquals("Expected " + groupSize + "  trace(s)", groupSize, traces.size());
            }
        }
        long averageDuration = totalDuration / (startEndPairs.size() * RETRIEVALS);
        logger.info("Average Duration " + averageDuration + " ms.");
    }

    public void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException ex) {
            logger.error("Exception,", ex);
        }
    }
}
