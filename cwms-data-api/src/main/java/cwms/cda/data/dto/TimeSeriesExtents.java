/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package cwms.cda.data.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonFormat.Shape;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import io.swagger.v3.oas.annotations.media.Schema;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;

@JsonRootName("extents")
@Schema(description = "TimeSeries extent information")
@JsonPropertyOrder(alphabetic = true)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
public class TimeSeriesExtents extends TimeExtents {

    @Schema(description = "TimeSeries version to which this extent information applies")
    @JsonFormat(shape = Shape.STRING)
    ZonedDateTime versionTime;

    @Schema(description = "Last update in the timeseries")
    @JsonFormat(shape = Shape.STRING)
    ZonedDateTime lastUpdate;

    @SuppressWarnings("unused") // required so JAXB can initialize and marshal
    private TimeSeriesExtents() {
        super(new Builder());
    }

    public TimeSeriesExtents(final ZonedDateTime versionTime, final ZonedDateTime earliestTime,
                             final ZonedDateTime latestTime, final ZonedDateTime lastUpdateTime) {
        super(new TimeExtents.Builder()
                .withEarliestTime(earliestTime)
                .withLatestTime(latestTime));
        this.versionTime = versionTime;
        this.lastUpdate = lastUpdateTime;
    }

    public TimeSeriesExtents(final Timestamp versionTime, final Timestamp earliestTime,
                             final Timestamp latestTime, final Timestamp lastUpdateTime) {
        this(toZdt(versionTime), toZdt(earliestTime), toZdt(latestTime), toZdt(lastUpdateTime));
    }

    private static ZonedDateTime toZdt(final Timestamp time) {
        if (time != null) {
            return ZonedDateTime.ofInstant(time.toInstant(), ZoneId.of("UTC"));
        } else {
            return null;
        }
    }

    public ZonedDateTime getVersionTime() {
        return this.versionTime;
    }

    public ZonedDateTime getLastUpdate() {
        return this.lastUpdate;
    }

}
