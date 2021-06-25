package com.linkedin.kafka.cruisecontrol.monitor.sampling.newrelic;

import com.linkedin.kafka.cruisecontrol.metricsreporter.metric.RawMetricType;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.mock;

public class NewRelicMetricSamplerTest {
    private static final double DOUBLE_DELTA = 0.00000001;
    private static final double BYTES_IN_KB = 1024.0;

    private static final int FIXED_VALUE = 94;
    private static final long START_EPOCH_SECONDS = 1603301400L;
    private static final long START_TIME_MS = TimeUnit.SECONDS.toMillis(START_EPOCH_SECONDS);
    private static final long END_TIME_MS = START_TIME_MS + TimeUnit.SECONDS.toMillis(59);

    private static final int TOTAL_BROKERS = 3;
    private static final int TOTAL_PARTITIONS = 3;

    private static final String TEST_TOPIC = "test-topic";
    private static final String TEST_TOPIC_WITH_DOT = "test.topic";

    private NewRelicMetricSampler _newRelicMetricSampler;
    private NewRelicAdapter _newRelicAdapter;
    private Map<RawMetricType.MetricScope, String> _queryMap;

    /**
     * Set up mocks
     */
    @Before
    public void setUp() {
        _newRelicAdapter = mock(NewRelicAdapter.class);
        _newRelicMetricSampler = new NewRelicMetricSampler();
        _queryMap = new NewRelicQuerySupplier().get();
    }
}
