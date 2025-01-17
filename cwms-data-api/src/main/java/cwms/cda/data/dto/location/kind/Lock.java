/*
 * MIT License
 * Copyright (c) 2024 Hydrologic Engineering Center
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dto.location.kind;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV1;

@FormattableWith(contentType = Formats.JSONV1, formatter = JsonV1.class, aliases = {Formats.DEFAULT, Formats.JSON})
@JsonDeserialize(builder = Lock.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonPropertyOrder({"projectId", "location", "chamber-type", "lock-width", "lock-length", "normal-lock-lift",
    "maximum-lock-lift", "length-units", "volume-units", "volume-per-lockage", "minimum-draft",
    "high-water-upper-pool-location-level", "low-water-lower-pool-location-level",
    "high-water-lower-pool-location-level", "low-water-upper-pool-location-level",
    "high-water-upper-pool-warning-level", "high-water-lower-pool-warning-level"})
public class Lock extends CwmsDTOBase {
    @JsonProperty(required = true)
    private final CwmsId projectId;
    @JsonProperty(required = true)
    private final Location location;
    private final LookupType chamberType;
    private final double lockWidth;
    private final double lockLength;
    private final double normalLockLift;
    private final double volumePerLockage;
    private final double minimumDraft;
    private final double maximumLockLift;
    private final String lengthUnits;
    private final String volumeUnits;
    private final String elevationUnits;
    private final LockLocationLevelRef highWaterUpperPoolLocationLevel;
    private final LockLocationLevelRef lowWaterLowerPoolLocationLevel;
    private final LockLocationLevelRef highWaterLowerPoolLocationLevel;
    private final LockLocationLevelRef lowWaterUpperPoolLocationLevel;
    private final Double highWaterUpperPoolWarningLevel;
    private final Double highWaterLowerPoolWarningLevel;

    private Lock(Builder builder) {
        this.location = builder.location;
        this.projectId = builder.projectId;
        this.chamberType = builder.chamberType;
        this.lockWidth = builder.lockWidth;
        this.lockLength = builder.lockLength;
        this.normalLockLift = builder.normalLockLift;
        this.volumePerLockage = builder.volumePerLockage;
        this.minimumDraft = builder.minimumDraft;
        this.highWaterUpperPoolLocationLevel = builder.highWaterUpperPoolLocationLevel;
        this.lowWaterLowerPoolLocationLevel = builder.lowWaterLowerPoolLocationLevel;
        this.highWaterLowerPoolLocationLevel = builder.highWaterLowerPoolLocationLevel;
        this.lowWaterUpperPoolLocationLevel = builder.lowWaterUpperPoolLocationLevel;
        this.maximumLockLift = builder.maximumLockLift;
        this.lengthUnits = builder.lengthUnits;
        this.volumeUnits = builder.volumeUnits;
        this.elevationUnits = builder.elevationUnits;
        this.highWaterUpperPoolWarningLevel = builder.highWaterUpperPoolWarningLevel;
        this.highWaterLowerPoolWarningLevel = builder.highWaterLowerPoolWarningLevel;
    }

    public final CwmsId getProjectId() {
        return projectId;
    }

    public final Location getLocation() {
        return location;
    }

    public final LookupType getChamberType() {
        return chamberType;
    }

    public final double getLockWidth() {
        return lockWidth;
    }

    public final double getLockLength() {
        return lockLength;
    }

    public final double getNormalLockLift() {
        return normalLockLift;
    }

    public final double getMaximumLockLift() {
        return maximumLockLift;
    }

    public final String getLengthUnits() {
        return lengthUnits;
    }

    public final String getVolumeUnits() {
        return volumeUnits;
    }

    public final String getElevationUnits() {
        return elevationUnits;
    }

    public final double getVolumePerLockage() {
        return volumePerLockage;
    }

    public final double getMinimumDraft() {
        return minimumDraft;
    }

    public final LockLocationLevelRef getHighWaterUpperPoolLocationLevel() {
        return highWaterUpperPoolLocationLevel;
    }

    public final LockLocationLevelRef getLowWaterLowerPoolLocationLevel() {
        return lowWaterLowerPoolLocationLevel;
    }

    public final LockLocationLevelRef getHighWaterLowerPoolLocationLevel() {
        return highWaterLowerPoolLocationLevel;
    }

    public final LockLocationLevelRef getLowWaterUpperPoolLocationLevel() {
        return lowWaterUpperPoolLocationLevel;
    }

    public final Double getHighWaterUpperPoolWarningLevel() {
        return highWaterUpperPoolWarningLevel;
    }

    public final Double getHighWaterLowerPoolWarningLevel() {
        return highWaterLowerPoolWarningLevel;
    }

    public static final class Builder {
        private Location location;
        private CwmsId projectId;
        private LookupType chamberType;
        private double lockWidth;
        private double lockLength;
        private double normalLockLift;
        private double maximumLockLift;
        private double volumePerLockage;
        private double minimumDraft;
        private String lengthUnits;
        private String volumeUnits;
        private String elevationUnits;
        private LockLocationLevelRef highWaterUpperPoolLocationLevel;
        private LockLocationLevelRef lowWaterLowerPoolLocationLevel;
        private LockLocationLevelRef highWaterLowerPoolLocationLevel;
        private LockLocationLevelRef lowWaterUpperPoolLocationLevel;
        private Double highWaterUpperPoolWarningLevel;
        private Double highWaterLowerPoolWarningLevel;

        public Builder withLocation(Location location) {
            this.location = location;
            return this;
        }

        public Builder withProjectId(CwmsId projectId) {
            this.projectId = projectId;
            return this;
        }

        public Builder withChamberType(LookupType chamberType) {
            this.chamberType = chamberType;
            return this;
        }

        public Builder withLockWidth(double lockWidth) {
            this.lockWidth = lockWidth;
            return this;
        }

        public Builder withLockLength(double lockLength) {
            this.lockLength = lockLength;
            return this;
        }

        public Builder withNormalLockLift(double normalLockLift) {
            this.normalLockLift = normalLockLift;
            return this;
        }

        public Builder withMaximumLockLift(double maximumLockLift) {
            this.maximumLockLift = maximumLockLift;
            return this;
        }

        public Builder withLengthUnits(String units) {
            this.lengthUnits = units;
            return this;
        }

        public Builder withVolumeUnits(String volumeUnits) {
            this.volumeUnits = volumeUnits;
            return this;
        }

        public Builder withElevationUnits(String elevationUnits) {
            this.elevationUnits = elevationUnits;
            return this;
        }

        public Builder withVolumePerLockage(double volumePerLockage) {
            this.volumePerLockage = volumePerLockage;
            return this;
        }

        public Builder withMinimumDraft(double minimumDraft) {
            this.minimumDraft = minimumDraft;
            return this;
        }

        public Builder withHighWaterUpperPoolLocationLevel(LockLocationLevelRef highWaterUpperPoolLocationLevel) {
            this.highWaterUpperPoolLocationLevel = highWaterUpperPoolLocationLevel;
            return this;
        }

        public Builder withLowWaterLowerPoolLocationLevel(LockLocationLevelRef lowWaterLowerPoolLocationLevel) {
            this.lowWaterLowerPoolLocationLevel = lowWaterLowerPoolLocationLevel;
            return this;
        }

        public Builder withHighWaterLowerPoolLocationLevel(LockLocationLevelRef highWaterLowerPoolLocationLevel) {
            this.highWaterLowerPoolLocationLevel = highWaterLowerPoolLocationLevel;
            return this;
        }

        public Builder withLowWaterUpperPoolLocationLevel(LockLocationLevelRef lowWaterUpperPoolLocationLevel) {
            this.lowWaterUpperPoolLocationLevel = lowWaterUpperPoolLocationLevel;
            return this;
        }

        public Builder withHighWaterUpperPoolWarningLevel(Double highWaterUpperPoolWarningLevel) {
            this.highWaterUpperPoolWarningLevel = highWaterUpperPoolWarningLevel;
            return this;
        }

        public Builder withHighWaterLowerPoolWarningLevel(Double highWaterLowerPoolWarningLevel) {
            this.highWaterLowerPoolWarningLevel = highWaterLowerPoolWarningLevel;
            return this;
        }

        public Lock build() {
            return new Lock(this);
        }
    }
}
