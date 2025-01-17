package cwms.cda.data.dto.timeseriesprofile;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import cwms.cda.api.errors.RequiredFieldException;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.CwmsDTOValidator;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class)
@JsonDeserialize(builder = TimeSeriesProfileInstance.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public final class TimeSeriesProfileInstance extends CwmsDTOPaginated {
    private final TimeSeriesProfile timeSeriesProfile;
    private final List<ParameterColumnInfo> parameterColumns;
    private final List<DataColumnInfo> dataColumns;
    @JsonFormat(shape = JsonFormat.Shape.ARRAY)
    @JsonSerialize(using = TimeSeriesDataSerializer.class)
    private final Map<Long, List<TimeSeriesData>> timeSeriesList;
    private final String locationTimeZone;
    private final String version;
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private final Instant versionDate;
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private final Instant firstDate;
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private final Instant lastDate;
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private final Instant pageFirstDate;
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private final Instant pageLastDate;

    private TimeSeriesProfileInstance(Builder builder) {
        super(builder.page, builder.pageSize, builder.total);
        timeSeriesList = builder.timeSeriesList;
        parameterColumns = builder.parameterColumns;
        dataColumns = builder.dataColumns;
        locationTimeZone = builder.locationTimeZone;
        timeSeriesProfile = builder.timeSeriesProfile;
        version = builder.version;
        versionDate = builder.versionDate;
        firstDate = builder.firstDate;
        lastDate = builder.lastDate;
        pageFirstDate = builder.pageFirstDate;
        pageLastDate = builder.pageLastDate;
        super.nextPage = builder.nextPage;
    }

    public TimeSeriesProfile getTimeSeriesProfile() {
        return timeSeriesProfile;
    }

    public Map<Long, List<TimeSeriesData>> getTimeSeriesList() {
        return timeSeriesList;
    }

    public List<ParameterColumnInfo> getParameterColumns() {
        return parameterColumns;
    }

    public List<DataColumnInfo> getDataColumns() {
        return dataColumns;
    }

    public String getVersion() {
        return version;
    }

    public Instant getVersionDate() {
        return versionDate;
    }

    public Instant getFirstDate() {
        return firstDate;
    }

    public Instant getLastDate() {
        return lastDate;
    }

    public String getLocationTimeZone() {
        return locationTimeZone;
    }

    public Instant getPageFirstDate() {
        return pageFirstDate;
    }

    public Instant getPageLastDate() {
        return pageLastDate;
    }

    @Override
    protected void validateInternal(CwmsDTOValidator validator) {
        if (timeSeriesProfile == null) {
            throw new RequiredFieldException("timeSeriesProfile");
        }
    }

    @JsonPOJOBuilder
    @JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
    public static final class Builder {
        private Map<Long, List<TimeSeriesData>> timeSeriesList = new TreeMap<>();
        private TimeSeriesProfile timeSeriesProfile;
        private String version;
        private Instant versionDate;
        private Instant firstDate;
        private Instant lastDate;
        private List<ParameterColumnInfo> parameterColumns;
        private List<DataColumnInfo> dataColumns;
        private String locationTimeZone;
        private Instant pageFirstDate;
        private Instant pageLastDate;
        private String page;
        private int pageSize;
        private int total;
        private String nextPage;

        public TimeSeriesProfileInstance.Builder withTimeSeriesProfile(TimeSeriesProfile timeSeriesProfile) {
            this.timeSeriesProfile = timeSeriesProfile;
            return this;
        }

        public TimeSeriesProfileInstance.Builder withTimeSeriesList(Map<Long, List<TimeSeriesData>> timeSeriesList) {
            this.timeSeriesList = timeSeriesList;
            return this;
        }

        public TimeSeriesProfileInstance.Builder withVersion(String version) {
            this.version = version;
            return this;
        }

        public TimeSeriesProfileInstance.Builder withVersionDate(Instant instant) {
            this.versionDate = instant;
            return this;
        }

        public TimeSeriesProfileInstance.Builder withFirstDate(Instant instant) {
            this.firstDate = instant;
            return this;
        }

        public TimeSeriesProfileInstance.Builder withLastDate(Instant instant) {
            this.lastDate = instant;
            return this;
        }

        public TimeSeriesProfileInstance.Builder withParameterColumns(List<ParameterColumnInfo> parameterColumns) {
            this.parameterColumns = parameterColumns;
            return this;
        }

        public TimeSeriesProfileInstance.Builder withDataColumns(List<DataColumnInfo> dataColumns) {
            this.dataColumns = dataColumns;
            return this;
        }

        public TimeSeriesProfileInstance.Builder withLocationTimeZone(String locationTimeZone) {
            this.locationTimeZone = locationTimeZone;
            return this;
        }

        public TimeSeriesProfileInstance.Builder withPageFirstDate(Instant pageFirstDate) {
            this.pageFirstDate = pageFirstDate;
            return this;
        }

        public TimeSeriesProfileInstance.Builder withPageLastDate(Instant pageLastDate) {
            this.pageLastDate = pageLastDate;
            return this;
        }

        public TimeSeriesProfileInstance.Builder withPage(String page) {
            this.page = page;
            return this;
        }

        public TimeSeriesProfileInstance.Builder withPageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public TimeSeriesProfileInstance.Builder withTotal(int total) {
            this.total = total;
            return this;
        }

        public Builder withNextPage(String nextPage) {
            this.nextPage = nextPage;
            return this;
        }

        public TimeSeriesProfileInstance build() {
            return new TimeSeriesProfileInstance(this);
        }
    }
}
