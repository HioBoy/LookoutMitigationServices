package com.amazon.lookout.mitigation.service.activity.helper.mws;

import amazon.mws.data.Datapoint;
import amazon.mws.query.MonitoringQueryClient;
import amazon.mws.query.MonitoringQueryClientProvider;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import java.util.ArrayList;
import static org.junit.Assert.assertEquals;

public class MWSHelperTest {
  private static MonitoringQueryClient mwsQueryClient = Mockito.mock(MonitoringQueryClient.class);
  private static MonitoringQueryClientProvider mwsClientProvider = Mockito.mock(MonitoringQueryClientProvider.class);
  private static MWSHelper mwsHelper =  Mockito.mock(MWSHelper.class);

  private static MetricsFactory metricsFactory = Mockito.mock(MetricsFactory.class);
  private static Metrics metrics = Mockito.mock(Metrics.class);
  private static TSDMetrics tsdMetrics;

  private static String location = "brg-test-1";

  @BeforeClass
  public static void setUpOnce() {
    TestUtils.configureLogging();
    mwsQueryClient = mwsClientProvider.getClient();

    // mock TSDMetric
    Mockito.doReturn(metrics).when(metricsFactory).newMetrics();
    Mockito.doReturn(metrics).when(metrics).newMetrics();
    tsdMetrics = new TSDMetrics(metricsFactory);
  }

  /**
   * Test getBGPTotalAnnouncements for given location.
   * @throws MWSRequestException
   */
  @Test
  public void testGetBGPTotalAnnouncements() throws MWSRequestException {
    //This location does not exist and thats why always return an empty list
    assertEquals(new ArrayList<Datapoint>(), mwsHelper.getBGPTotalAnnouncements(location, tsdMetrics));
  }
}

