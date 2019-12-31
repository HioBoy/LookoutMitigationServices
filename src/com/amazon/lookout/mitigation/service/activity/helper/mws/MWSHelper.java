package com.amazon.lookout.mitigation.service.activity.helper.mws;

import java.beans.ConstructorProperties;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import amazon.mws.data.DateTimeRange;
import amazon.mws.data.DimensionConstants;
import amazon.mws.data.MetricSchema;
import amazon.mws.data.Statistic;
import amazon.mws.data.TimeRange;
import amazon.mws.query.MonitoringQueryClient;
import amazon.mws.request.GetMetricDataRequest;
import amazon.query.types.DateTime;
import lombok.NonNull;
import amazon.mws.query.MonitoringQueryClientProvider;
import amazon.mws.response.GetMetricDataResponse;
import amazon.mws.types.Stat;
import amazon.mws.types.StatPeriod;
import amazon.mws.data.Datapoint;

import com.amazon.aws158.commons.metric.TSDMetrics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MWSHelper {

    private static final Log LOG = LogFactory.getLog(MWSHelper.class);

    // This specifies how old data is to be retrieved in millis
    private static final long MWS_MAX_HISTORY_TO_FETCH = 300000;

    private final Map<String, String> mwsMetricBaseDimensions;

    private final MonitoringQueryClient mwsQueryClient;

    private static final String MWS_QUERY_SUCCESS_METRIC_KEY = "MWSDataQuerySuccess";
    private static final String MWS_QUERY_FAILED_METRIC_KEY = "MWSDataQueryFailed";
    private static final String NUM_MWS_QUERY_ATTEMPTS_KEY = "NumMWSQueryAttempts";

    private MWSHelper(MonitoringQueryClientProvider mwsClientProvider, Map<String, String> defaultDimensions) {
        this.mwsQueryClient = mwsClientProvider.getClient();
        this.mwsMetricBaseDimensions = defaultDimensions;
    }

    @ConstructorProperties({"mwsClientProvider", "mwsMetricDataset"})
    public MWSHelper(MonitoringQueryClientProvider mwsClientProvider,
                     String mwsMetricDataset) {
        this(mwsClientProvider, buildDefaultDimensions(mwsMetricDataset));
    }

    private static Map<String, String> buildDefaultDimensions() {

        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(DimensionConstants.HOST_GROUP_KEY, "ALL");
        dimensions.put(DimensionConstants.HOST_KEY, "ALL");
        dimensions.put(DimensionConstants.METHOD_NAME_KEY, "ALL");
        dimensions.put(DimensionConstants.CLIENT_NAME_KEY, "ALL");
        dimensions.put(DimensionConstants.METRIC_CLASS_KEY, "NONE");
        dimensions.put(DimensionConstants.INSTANCE_KEY, "NONE");

        return dimensions;
    }


    private static Map<String, String> buildDefaultDimensions(@NonNull String mwsMetricDataset) {
        Map<String, String> dimensions = buildDefaultDimensions();
        dimensions.put(DimensionConstants.DATASET_KEY, mwsMetricDataset);
        return dimensions;
    }

    /**
     * Retrieve data for single metric based on dimensions.
     *
     * @param dimensions - predefined values for some dimensions.
     * @param statPeriod - period of data aggregation.
     * @param stat       - metric statistic to retrieve
     * @param timeRange  - time interval to get data for
     * @param tsdMetrics - metrics object
     * @return data for specified metric
     * @throws Exception
     */
    private GetMetricDataResponse getDataForVaribles(Map<String, String> dimensions, StatPeriod statPeriod, Stat stat,
                                                     TimeRange timeRange, @NonNull TSDMetrics tsdMetrics) throws MWSRequestException {
        MetricSchema schema = new MetricSchema(DimensionConstants.SERVICE_METRIC_SCHEMA_KEY, dimensions);
        LOG.info("MWS request dimensions:" + dimensions + ", statPeriod: " + statPeriod);

        tsdMetrics.addZero(NUM_MWS_QUERY_ATTEMPTS_KEY);
        tsdMetrics.addZero(MWS_QUERY_SUCCESS_METRIC_KEY);
        tsdMetrics.addZero(MWS_QUERY_FAILED_METRIC_KEY);

        Statistic statistic = new Statistic(schema, statPeriod, stat);

        try {
            GetMetricDataRequest getMetricDataRequest = new GetMetricDataRequest(statistic, timeRange);
            tsdMetrics.addOne(NUM_MWS_QUERY_ATTEMPTS_KEY);
            GetMetricDataResponse response = (GetMetricDataResponse) mwsQueryClient
                    .requestResponse(getMetricDataRequest);
            LOG.info("MWS response: " + response.toString());
            tsdMetrics.addZero(MWS_QUERY_SUCCESS_METRIC_KEY);
            return response;
        } catch (Exception ex) {
            LOG.error("MWS request error: " + ex.getMessage());
            throw new MWSRequestException("MWS request failed", ex);
        }
    }

    public List<Datapoint> getBGPTotalAnnouncements(String location, TSDMetrics tsdMetrics) throws MWSRequestException {
        String metricName = "AnnouncedCount";
        String serviceName = "BlackWatchBGPController";
        StatPeriod statPeriod = StatPeriod.OneMinute;
        Stat stat = Stat.sum;
        String methodName = "AnnouncedRoutes";
        DateTime startTime = new DateTime(System.currentTimeMillis() - MWS_MAX_HISTORY_TO_FETCH);
        DateTime endTime = new DateTime(System.currentTimeMillis());
        TimeRange timeRange = new DateTimeRange(startTime, endTime);

        Map<String, String> dimensions = new HashMap<>(mwsMetricBaseDimensions);
        dimensions.put(DimensionConstants.MARKETPLACE_KEY, location.toUpperCase());
        dimensions.put(DimensionConstants.SERVICE_NAME_KEY, serviceName);
        dimensions.put(DimensionConstants.METRIC_KEY, metricName);
        dimensions.put(DimensionConstants.METHOD_NAME_KEY, methodName);

        GetMetricDataResponse response = getDataForVaribles(dimensions, statPeriod, stat, timeRange, tsdMetrics);
        return response.getStatisticSeries().get(0).getDatapoints();
    }
}