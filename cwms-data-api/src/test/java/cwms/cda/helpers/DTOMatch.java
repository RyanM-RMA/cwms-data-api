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

package cwms.cda.helpers;

import cwms.cda.data.dto.CwmsIdTimeExtentsEntry;
import cwms.cda.data.dto.TimeExtents;
import cwms.cda.data.dto.TimeSeriesExtents;
import cwms.cda.data.dto.location.kind.Lock;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.data.dto.location.kind.GateChange;
import cwms.cda.data.dto.location.kind.GateSetting;
import cwms.cda.data.dto.location.kind.LockLocationLevelRef;
import cwms.cda.data.dto.location.kind.Setting;
import cwms.cda.data.dto.AssignedLocation;
import cwms.cda.data.dto.location.kind.VirtualOutlet;
import cwms.cda.data.dto.measurement.Measurement;
import cwms.cda.data.dto.measurement.StreamflowMeasurement;
import cwms.cda.data.dto.measurement.SupplementalStreamflowMeasurement;
import cwms.cda.data.dto.measurement.UsgsMeasurement;
import cwms.cda.data.dto.stream.StreamLocationNode;

import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.VirtualOutletRecord;
import cwms.cda.data.dto.location.kind.Embankment;
import cwms.cda.data.dto.location.kind.Outlet;
import cwms.cda.data.dto.location.kind.Turbine;
import cwms.cda.data.dto.location.kind.TurbineChange;
import cwms.cda.data.dto.location.kind.TurbineSetting;
import cwms.cda.data.dto.stream.Stream;
import cwms.cda.data.dto.stream.StreamLocation;
import cwms.cda.data.dto.stream.StreamNode;
import cwms.cda.data.dto.stream.StreamReach;
import cwms.cda.data.dto.watersupply.PumpLocation;
import cwms.cda.data.dto.watersupply.PumpTransfer;
import cwms.cda.data.dto.watersupply.WaterSupplyAccounting;
import cwms.cda.data.dto.watersupply.WaterSupplyPump;
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;

import static org.junit.jupiter.api.Assertions.*;


@SuppressWarnings({"LongLine", "checkstyle:LineLength"})
public final class DTOMatch {

    private static final double DEFAULT_DELTA = 0.0001;

    private DTOMatch() {
        throw new AssertionError("Utility class");
    }

    public static void assertMatch(CwmsId first, CwmsId second, String variableName) {
        assertAll(
            () -> Assertions.assertEquals(first.getOfficeId(), second.getOfficeId(),variableName + " is not the same. Office ID differs"),
            () -> Assertions.assertEquals(first.getName(), second.getName(),variableName + " is not the same. Name differs")
        );
    }

    public static void assertMatch(CwmsId first, CwmsId second) {
        assertMatch(first, second, "CwmsId");
    }


    public static void assertMatch(StreamLocation streamLocation, StreamLocation deserialized) {
        assertAll(
            () -> assertMatch(streamLocation.getStreamLocationNode(), deserialized.getStreamLocationNode()),
            () -> assertEquals(streamLocation.getPublishedStation(), deserialized.getPublishedStation(), "The published station does not match"),
            () -> assertEquals(streamLocation.getNavigationStation(), deserialized.getNavigationStation(), "The navigation station does not match"),
            () -> assertEquals(streamLocation.getLowestMeasurableStage(), deserialized.getLowestMeasurableStage(), "The lowest measurable stage does not match"),
            () -> assertEquals(streamLocation.getTotalDrainageArea(), deserialized.getTotalDrainageArea(), "The total drainage area does not match"),
            () -> assertEquals(streamLocation.getUngagedDrainageArea(), deserialized.getUngagedDrainageArea(), "The ungaged drainage area does not match"),
            () -> assertEquals(streamLocation.getAreaUnits(), deserialized.getAreaUnits(), "The area unit does not match"),
            () -> assertEquals(streamLocation.getStageUnits(), deserialized.getStageUnits(), "The stage unit does not match")
        );
    }

    public static void assertMatch(StreamLocationNode streamLocationNode, StreamLocationNode streamLocationNode1) {
        assertAll(
            () -> assertMatch(streamLocationNode.getId(), streamLocationNode1.getId()),
            () -> assertMatch(streamLocationNode.getStreamNode(), streamLocationNode1.getStreamNode())
        );
    }

    public static void assertMatch(StreamNode node1, StreamNode node2) {
        assertAll(
            () -> assertMatch(node1.getStreamId(), node2.getStreamId(), "Stream ID does not match"),
            () -> assertEquals(node1.getBank(), node2.getBank(), "The bank does not match"),
            () -> assertEquals(node1.getStation(), node2.getStation(), "The station does not match"),
            () -> assertEquals(node1.getStationUnits(), node2.getStationUnits(), "The station unit does not match")
        );
    }

    public static void assertMatch(StreamReach reach1, StreamReach reach2) {
        assertAll(
            () -> assertEquals(reach1.getComment(), reach2.getComment(), "Comments do not match"),
            () -> assertMatch(reach1.getDownstreamNode(), reach2.getDownstreamNode()),
            () -> assertMatch(reach1.getUpstreamNode(), reach2.getUpstreamNode()),
            () -> assertMatch(reach1.getConfigurationId(), reach2.getConfigurationId(),"Configuration ID does not match"),
            () -> assertMatch(reach1.getStreamId(), reach2.getStreamId(), "Stream ID does not match"),
            () -> assertMatch(reach1.getId(), reach2.getId(), "Stream reach ID does not match")
        );
    }

    public static void assertMatch(Stream stream1, Stream stream2) {
        assertAll(
            () -> assertEquals(stream1.getStartsDownstream(), stream2.getStartsDownstream(),"Starts downstream property does not match"),
            () -> assertMatch(stream1.getFlowsIntoStreamNode(), stream2.getFlowsIntoStreamNode()),
            () -> assertMatch(stream1.getDivertsFromStreamNode(), stream2.getDivertsFromStreamNode()),
            () -> assertEquals(stream1.getLength(), stream2.getLength(), "Length does not match"),
            () -> assertEquals(stream1.getAverageSlope(), stream2.getAverageSlope(), "Average slope does not match"),
            () -> assertEquals(stream1.getLengthUnits(), stream2.getLengthUnits(), "Length units do not match"),
            () -> assertEquals(stream1.getSlopeUnits(), stream2.getSlopeUnits(), "Slope units do not match"),
            () -> assertEquals(stream1.getComment(), stream2.getComment(), "Comment does not match"),
            () -> assertMatch(stream1.getId(), stream2.getId(), "Stream ID does not match")
        );
    }

    public static void assertMatch(Embankment first, Embankment second) {

        assertAll(
            () -> assertEquals(first.getUpstreamSideSlope(), second.getUpstreamSideSlope(),"Upstream side slope doesn't match"),
            () -> assertEquals(first.getDownstreamSideSlope(), second.getDownstreamSideSlope(),"Downstream side slope doesn't match"),
            () -> assertEquals(first.getStructureLength(), second.getStructureLength(),"Structure length doesn't match"),
            () -> assertEquals(first.getMaxHeight(), second.getMaxHeight(), "Maximum height doesn't match"),
            () -> assertEquals(first.getTopWidth(), second.getTopWidth(), "Top width doesn't match"),
            () -> assertEquals(first.getLengthUnits(), second.getLengthUnits(), "Units ID doesn't match"),
            () -> assertMatch(first.getDownstreamProtectionType(), second.getDownstreamProtectionType()),
            () -> assertMatch(first.getUpstreamProtectionType(), second.getUpstreamProtectionType()),
            () -> assertMatch(first.getStructureType(), second.getStructureType()),
            () -> assertEquals(first.getLocation(), second.getLocation(), "Location doesn't match"),
            () -> assertMatch(first.getProjectId(), second.getProjectId(), "Project ID does not match")
        );
    }

    public static void assertMatch(LookupType lookupType, LookupType deserialized) {
        assertEquals(lookupType.getOfficeId(), deserialized.getOfficeId(), "Office IDs do not match");
        assertEquals(lookupType.getDisplayValue(), deserialized.getDisplayValue(), "Display values do not match");
        assertEquals(lookupType.getTooltip(), deserialized.getTooltip(), "Tool tips do not match");
        assertEquals(lookupType.getActive(), deserialized.getActive(), "Active status does not match");
    }

    public static void assertMatch(Turbine first, Turbine second) {
        assertAll(
            () -> assertMatch(first.getProjectId(), second.getProjectId(), "Project IDs do not match"),
            () -> assertEquals(first.getLocation(), second.getLocation(), "Locations are not the same")
        );
    }

    public static void assertMatch(TurbineChange first, TurbineChange second) {
        assertAll(() -> assertMatch(first.getProjectId(), second.getProjectId()),
            () -> assertMatch(first.getReasonType(), second.getReasonType()),
            () -> assertMatch(first.getDischargeComputationType(), second.getDischargeComputationType()),
            () -> assertSettingsMatch(first.getSettings(), second.getSettings(), DTOMatch::assertMatch, "Turbine"),
            () -> assertEquals(first.getChangeDate(), second.getChangeDate(), "Change dates do not match"),
            () -> assertEquals(first.getDischargeUnits(), second.getDischargeUnits(), "Discharge units do not match"),
            () -> assertEquals(first.getNewTotalDischargeOverride(), second.getNewTotalDischargeOverride(),"New total discharge override does not match"),
            () -> assertEquals(first.getOldTotalDischargeOverride(), second.getOldTotalDischargeOverride(),"Old total discharge override does not match"),
            () -> assertEquals(first.getElevationUnits(), second.getElevationUnits(), "Elevation units do not match"),
            () -> assertEquals(first.getTailwaterElevation(), second.getTailwaterElevation(),"Tailwater elevations do not match"),
            () -> assertEquals(first.getPoolElevation(), second.getPoolElevation(), "Pool elevations do not match"),
            () -> assertEquals(first.getNotes(), second.getNotes(), "Notes do not match"),
            () -> assertEquals(first.isProtected(), second.isProtected(), "Protected status does not match"));
    }

    public static <T extends Setting> void assertSettingsMatch(Set<T> first, Set<T> second,
                                                               AssertMatchMethod<T> matcher, String settingType) {
        List<Executable> assertions = new ArrayList<>();
        assertions.add(() -> assertEquals(first.size(), second.size(), settingType + " settings lists sizes do not match"));
        for (T setting : first) {
            assertions.add(() -> {
                T match = second.stream()
                                .filter(s -> s.getLocationId()
                                              .getOfficeId()
                                              .equalsIgnoreCase(setting.getLocationId().getOfficeId()))
                                .filter(s -> s.getLocationId()
                                              .getName()
                                              .equalsIgnoreCase(setting.getLocationId().getName()))
                                .findFirst()
                                .orElseThrow(() -> fail("Setting " + setting.getLocationId().getName() + " not found"));
                matcher.assertMatch(setting, match);
            });
        }

        assertAll(assertions.toArray(new Executable[]{}));
    }

    public static void assertMatch(TurbineSetting first, TurbineSetting second) {
        assertAll(
            () -> assertEquals(first.getDischargeUnits(), second.getDischargeUnits(), "Discharge units do not match"),
            () -> assertEquals(first.getNewDischarge(), second.getNewDischarge(), "New discharge does not match"),
            () -> assertEquals(first.getOldDischarge(), second.getOldDischarge(), "Old discharge does not match"),
            () -> assertEquals(first.getGenerationUnits(), second.getGenerationUnits(),"Generation units do not match"),
            () -> assertEquals(first.getRealPower(), second.getRealPower(), "Real power does not match"),
            () -> assertEquals(first.getScheduledLoad(), second.getScheduledLoad(), "Scheduled load does not match"));
    }

    public static void assertMatch(Outlet first, Outlet second) {
        assertAll(
                () -> assertMatch(first.getProjectId(), second.getProjectId()),
                () -> assertEquals(first.getLocation(), second.getLocation()),
                () -> assertMatch(first.getRatingGroupId(), second.getRatingGroupId())
        );
    }

    public static void assertMatch(VirtualOutletRecord first, VirtualOutletRecord second) {
        assertAll(() -> assertMatch(first.getOutletId(), second.getOutletId()),
                  () -> assertMatch(first.getDownstreamOutletIds(), second.getDownstreamOutletIds(), DTOMatch::assertMatch)
        );
    }

    public static void assertMatch(VirtualOutlet first, VirtualOutlet second) {
        assertAll(() -> assertMatch(first.getVirtualOutletId(), second.getVirtualOutletId()),
                  () -> assertMatch(first.getVirtualRecords(), second.getVirtualRecords(), DTOMatch::assertMatch));
    }

    public static <T> void assertMatch(List<T> first, List<T> second, AssertMatchMethod<T> matcher) {
        assertAll(() -> assertEquals(first.size(), second.size()),
                  () -> assertAll(IntStream.range(0, first.size())
                                           .mapToObj(i -> () -> matcher.assertMatch(first.get(i), second.get(i)))));
    }

    public static void assertMatch(WaterSupplyPump firstPump, WaterSupplyPump secondPump) {
        assertAll(
                () -> assertMatch(firstPump.getPumpLocation(), secondPump.getPumpLocation()),
                () -> assertEquals(firstPump.getPumpType(), secondPump.getPumpType())
        );
    }

    public static void assertMatch(WaterUser firstUser, WaterUser secondUser) {
        assertAll(
                () -> assertEquals(firstUser.getEntityName(), secondUser.getEntityName()),
                () -> DTOMatch.assertMatch(firstUser.getProjectId(), secondUser.getProjectId()),
                () -> assertEquals(firstUser.getWaterRight(), secondUser.getWaterRight())
        );
    }

    public static void assertMatch(Location first, Location second) {
        assertAll(
                () -> assertEquals(first.getName(), second.getName()),
                () -> assertEquals(first.getLatitude(), second.getLatitude()),
                () -> assertEquals(first.getLongitude(), second.getLongitude()),
                () -> assertEquals(first.getHorizontalDatum(), second.getHorizontalDatum()),
                () -> assertEquals(first.getElevation(), second.getElevation()),
                () -> assertEquals(first.getElevationUnits(), second.getElevationUnits()),
                () -> assertEquals(first.getVerticalDatum(), second.getVerticalDatum()),
                () -> assertEquals(first.getPublicName(), second.getPublicName()),
                () -> assertEquals(first.getLongName(), second.getLongName()),
                () -> assertEquals(first.getDescription(), second.getDescription()),
                () -> assertEquals(first.getActive(), second.getActive()),
                () -> assertEquals(first.getLocationKind(), second.getLocationKind()),
                () -> assertEquals(first.getMapLabel(), second.getMapLabel()),
                () -> assertEquals(first.getPublishedLatitude(), second.getPublishedLatitude()),
                () -> assertEquals(first.getPublishedLongitude(), second.getPublishedLongitude()),
                () -> assertEquals(first.getBoundingOfficeId(), second.getBoundingOfficeId()),
                () -> assertEquals(first.getNation(), second.getNation()),
                () -> assertEquals(first.getNearestCity(), second.getNearestCity()),
                () -> assertEquals(first.getStateInitial(), second.getStateInitial()),
                () -> assertEquals(first.getCountyName(), second.getCountyName()),
                () -> assertEquals(first.getTimezoneName(), second.getTimezoneName()),
                () -> assertEquals(first.getOfficeId(), second.getOfficeId()),
                () -> assertEquals(first.getLocationType(), second.getLocationType())
        );
    }

    public static void assertMatch(WaterUserContract firstContract, WaterUserContract secondContract) {
        assertAll(
                () -> assertMatch(firstContract.getWaterUser(), secondContract.getWaterUser()),
                () -> DTOMatch.assertMatch(firstContract.getContractId(), secondContract.getContractId()),
                () -> DTOMatch.assertMatch(firstContract.getContractType(), secondContract.getContractType()),
                () -> assertEquals(firstContract.getContractEffectiveDate().getEpochSecond(),
                        secondContract.getContractEffectiveDate().getEpochSecond()),
                () -> assertEquals(firstContract.getContractExpirationDate().getEpochSecond(),
                        secondContract.getContractExpirationDate().getEpochSecond()),
                () -> assertEquals(firstContract.getContractedStorage(), secondContract.getContractedStorage()),
                () -> assertEquals(firstContract.getInitialUseAllocation(), secondContract.getInitialUseAllocation()),
                () -> assertEquals(firstContract.getFutureUseAllocation(), secondContract.getFutureUseAllocation()),
                () -> assertEquals(firstContract.getStorageUnitsId(), secondContract.getStorageUnitsId()),
                () -> assertEquals(firstContract.getFutureUsePercentActivated(),
                        secondContract.getFutureUsePercentActivated()),
                () -> assertEquals(firstContract.getTotalAllocPercentActivated(),
                        secondContract.getTotalAllocPercentActivated()),
                () -> assertMatch(firstContract.getPumpOutLocation(), secondContract.getPumpOutLocation()),
                () -> assertMatch(firstContract.getPumpOutBelowLocation(), secondContract.getPumpOutBelowLocation()),
                () -> assertMatch(firstContract.getPumpInLocation(), secondContract.getPumpInLocation())
        );
    }

    public static void assertMatch(AssignedLocation first, AssignedLocation second) {
        assertAll(
                () -> assertEquals(first.getAliasId(), second.getAliasId()),
                () -> assertEquals(first.getRefLocationId(), second.getRefLocationId()),
                () -> assertEquals(first.getOfficeId(), second.getOfficeId()),
                () -> assertEquals(first.getLocationId(), second.getLocationId()),
                () -> assertEquals(first.getAttribute(), second.getAttribute())
        );
    }

    public static void assertMatch(GateSetting first, GateSetting second) {
        assertAll(
                () -> assertMatch(first.getLocationId(), second.getLocationId(), "Location ID"),
                () -> assertMatch(first.getInvertElevation(), second.getInvertElevation(), "Invert Elevation"),
                () -> assertMatch(first.getOpening(), second.getOpening(), "Opening"),
                () -> assertEquals(first.getOpeningParameter(), second.getOpeningParameter(), "Opening Parameter"),
                () -> assertEquals(first.getOpeningUnits(), second.getOpeningUnits(), "Opening Units")
        );
    }

    public static void assertMatch(GateChange first, GateChange second) {
        assertAll(
                () -> assertEquals(first.getPoolElevation(), second.getPoolElevation(), "Pool Elevation"),
                () -> assertMatch(first.getReferenceElevation(), second.getReferenceElevation(), "Reference Elevation"),
                () -> assertMatch(first.getProjectId(), second.getProjectId(), "Project ID"),
                () -> assertMatch(first.getReasonType(), second.getReasonType()),
                () -> assertMatch(first.getDischargeComputationType(), second.getDischargeComputationType()),
                () -> assertSettingsMatch(first.getSettings(), second.getSettings(), DTOMatch::assertMatch, "Gate"),
                () -> assertEquals(first.getChangeDate(), second.getChangeDate(), "Change dates do not match"),
                () -> assertEquals(first.getDischargeUnits(), second.getDischargeUnits(), "Discharge units do not match"),
                () -> assertMatch(first.getNewTotalDischargeOverride(), second.getNewTotalDischargeOverride(),"New total discharge override does not match"),
                () -> assertMatch(first.getOldTotalDischargeOverride(), second.getOldTotalDischargeOverride(),"Old total discharge override does not match"),
                () -> assertEquals(first.getElevationUnits(), second.getElevationUnits(), "Elevation units do not match"),
                () -> assertMatch(first.getTailwaterElevation(), second.getTailwaterElevation(),"Tailwater elevations do not match"),
                () -> assertMatch(first.getPoolElevation(), second.getPoolElevation(), "Pool elevations do not match"),
                () -> assertEquals(first.getNotes(), second.getNotes(), "Notes do not match"),
                () -> assertEquals(first.isProtected(), second.isProtected(), "Protected status does not match")
        );
    }

    private static void assertMatch(Double first, Double second, double delta, String message) {
        if (first != null && second != null) {
            assertEquals(first, second, delta, message);
        } else if (first != null || second != null) {
            assertEquals(first, second, message);
        }
    }

    private static void assertMatch(Double first, Double second, String message) {
        assertMatch(first, second, DEFAULT_DELTA, message);
    }

    public static void assertMatch(WaterSupplyAccounting first, WaterSupplyAccounting second) {
        assertAll(
            () -> assertEquals(first.getContractName(), second.getContractName()),
            () -> assertMatch(first.getWaterUser(), second.getWaterUser()),
            () -> assertMatch(first.getPumpAccounting(), second.getPumpAccounting()),
            () -> assertMatch(first.getPumpLocations(), second.getPumpLocations())
        );
    }

    public static void assertMatch(Map<Instant, List<PumpTransfer>> first, Map<Instant, List<PumpTransfer>> second) {
        assertAll(
            () -> assertEquals(first.size(), second.size(), "Pump accounting sizes do not match"),
            () -> first.forEach((key, value) -> {
                List<PumpTransfer> secondValue = second.get(key);
                if (secondValue == null) {
                    fail("Pump accounting key not found: " + key);
                }
                assertMatch(value, secondValue, DTOMatch::assertMatch);
            })
        );
    }

    public static void assertMatch(PumpLocation first, PumpLocation second) {
        assertAll(
            () -> {
                if (first != null && second != null && first.getPumpOut() != null && second.getPumpOut() != null) {
                    assertMatch(first.getPumpOut(), second.getPumpOut());
                } else if (!(first == null && second == null)
                        && !((first != null && first.getPumpOut() == null)
                            && (second != null && second.getPumpOut() == null))) {
                    fail("Pump out locations do not match");
                }
            },
            () -> {
                if (first != null && second != null && first.getPumpIn() != null && second.getPumpIn() != null) {
                    assertMatch(first.getPumpIn(), second.getPumpIn());
                } else if (!(first == null && second == null)
                        && !((first != null && first.getPumpIn() == null)
                            && (second != null && second.getPumpIn() == null))) {
                    fail("Pump in locations do not match");
                }
            },
            () -> {
                if (first != null && second != null && first.getPumpBelow() != null && second.getPumpBelow() != null) {
                    assertMatch(first.getPumpBelow(), second.getPumpBelow());
                } else if (!(first == null && second == null)
                        && !((first != null && first.getPumpBelow() == null)
                            && (second != null && second.getPumpBelow() == null))) {
                    fail("Pump below locations do not match");
                }
            }
        );
    }

    private static void assertMatch(PumpTransfer first, PumpTransfer second) {
        assertAll(
            () -> assertEquals(first.getTransferTypeDisplay(), second.getTransferTypeDisplay()),
            () -> assertEquals(first.getFlow(), second.getFlow()),
            () -> assertEquals(first.getComment(), second.getComment()),
            () -> assertEquals(first.getPumpType(), second.getPumpType())
        );
    }

    public static <T extends CwmsDTOBase> void assertContainsDto(List<T> values, T expectedDto,
                                                                 BiPredicate<T, T> identifier,
                                                                 AssertMatchMethod<T> dtoMatcher,
                                                                 String message) {
        T receivedValue = values.stream()
                                  .filter(dto -> identifier.test(dto, expectedDto))
                                  .findFirst()
                                  .orElse(null);
        assertNotNull(receivedValue, message);
        dtoMatcher.assertMatch(expectedDto, receivedValue);
    }

    public static <T extends CwmsDTOBase> void assertDoesNotContainDto(List<T> values, T missingDto,
                                                                       BiPredicate<T, T> identifier, String message) {
        T receivedValue = values.stream()
                                .filter(dto -> identifier.test(dto, missingDto))
                                .findFirst()
                                .orElse(null);
        assertNull(receivedValue, message);
    }

    public static void assertMatch(Measurement first, Measurement second) {
        //based on Measurement DTO
        assertAll(
                () -> assertMatch(first.getId(), second.getId()),
                () -> assertEquals(first.getAgency(), second.getAgency(), "Agency does not match"),
                () -> assertEquals(first.getHeightUnit(), second.getHeightUnit(), "Height unit does not match"),
                () -> assertEquals(first.getFlowUnit(), second.getFlowUnit(), "Flow unit does not match"),
                () -> assertEquals(first.getTempUnit(), second.getTempUnit(), "Temperature unit does not match"),
                () -> assertEquals(first.getVelocityUnit(), second.getVelocityUnit(), "Velocity unit does not match"),
                () -> assertEquals(first.getAreaUnit(), second.getAreaUnit(), "Area unit does not match"),
                () -> assertEquals(first.isUsed(), second.isUsed(), "Used status does not match"),
                () -> assertEquals(first.getAgency(), second.getAgency(), "Agency does not match"),
                () -> assertEquals(first.getParty(), second.getParty(), "Party does not match"),
                () -> assertEquals(first.getWmComments(), second.getWmComments(), "WM Comments do not match"),
                () -> assertEquals(first.getInstant(), second.getInstant(), "Instant does not match"),
                () -> assertEquals(first.getNumber(), second.getNumber(), "Number does not match"),
                () -> assertMatch(first.getStreamflowMeasurement(), second.getStreamflowMeasurement()),
                () -> assertMatch(first.getSupplementalStreamflowMeasurement(), second.getSupplementalStreamflowMeasurement()),
                () -> assertMatch(first.getUsgsMeasurement(), second.getUsgsMeasurement())
        );
    }

    public static void assertMatch(StreamflowMeasurement first, StreamflowMeasurement second) {
        assertAll(
                () -> assertEquals(first.getGageHeight(), second.getGageHeight(), DEFAULT_DELTA,"Gage height does not match"),
                () -> assertEquals(first.getFlow(), second.getFlow(), DEFAULT_DELTA, "Flow does not match"),
                () -> assertEquals(first.getQuality(), second.getQuality(), "Quality does not match")
        );
    }

    public static void assertMatch(SupplementalStreamflowMeasurement first, SupplementalStreamflowMeasurement second) {
        assertAll(
                () -> assertEquals(first.getChannelFlow(), second.getChannelFlow(), DEFAULT_DELTA, "Channel flow does not match"),
                () -> assertEquals(first.getOverbankFlow(), second.getOverbankFlow(), DEFAULT_DELTA, "Overbank flow does not match"),
                () -> assertEquals(first.getOverbankMaxDepth(), second.getOverbankMaxDepth(), DEFAULT_DELTA, "Overbank max depth does not match"),
                () -> assertEquals(first.getChannelMaxDepth(), second.getChannelMaxDepth(), DEFAULT_DELTA, "Channel max depth does not match"),
                () -> assertEquals(first.getAvgVelocity(), second.getAvgVelocity(), DEFAULT_DELTA, "Average velocity does not match"),
                () -> assertEquals(first.getSurfaceVelocity(), second.getSurfaceVelocity(), DEFAULT_DELTA, "Surface velocity does not match"),
                () -> assertEquals(first.getMaxVelocity(), second.getMaxVelocity(), DEFAULT_DELTA, "Max velocity does not match"),
                () -> assertEquals(first.getEffectiveFlowArea(), second.getEffectiveFlowArea(), DEFAULT_DELTA, "Effective flow area does not match"),
                () -> assertEquals(first.getCrossSectionalArea(), second.getCrossSectionalArea(), DEFAULT_DELTA, "Cross sectional area does not match"),
                () -> assertEquals(first.getMeanGage(), second.getMeanGage(), DEFAULT_DELTA, "Mean gage does not match"),
                () -> assertEquals(first.getTopWidth(), second.getTopWidth(), DEFAULT_DELTA, "Top width does not match"),
                () -> assertEquals(first.getMainChannelArea(), second.getMainChannelArea(), DEFAULT_DELTA, "Main channel area does not match"),
                () -> assertEquals(first.getOverbankArea(), second.getOverbankArea(), DEFAULT_DELTA, "Overbank area does not match")
        );
    }

    public static void assertMatch(UsgsMeasurement first, UsgsMeasurement second) {
        assertAll(
                () -> assertEquals(first.getRemarks(), second.getRemarks(), "Remarks do not match"),
                () -> assertEquals(first.getCurrentRating(), second.getCurrentRating(), "Current rating does not match"),
                () -> assertEquals(first.getControlCondition(), second.getControlCondition(), "Control condition does not match"),
                () -> assertEquals(first.getShiftUsed(), second.getShiftUsed(), DEFAULT_DELTA, "Shift used does not match"),
                () -> assertEquals(first.getPercentDifference(), second.getPercentDifference(), DEFAULT_DELTA, "Percent difference does not match"),
                () -> assertEquals(first.getFlowAdjustment(), second.getFlowAdjustment(), "Flow adjustment does not match"),
                () -> assertEquals(first.getDeltaHeight(), second.getDeltaHeight(), DEFAULT_DELTA, "Delta height does not match"),
                () -> assertEquals(first.getDeltaTime(), second.getDeltaTime(), "Delta time does not match"),
                () -> assertEquals(first.getAirTemp(), second.getAirTemp(), DEFAULT_DELTA, "Air temperature does not match"),
                () -> assertEquals(first.getWaterTemp(), second.getWaterTemp(), DEFAULT_DELTA, "Water temperature does not match")
        );
    }

    public static void assertMatch(Lock first, Lock second, boolean fromDB) {
        assertAll(
                () -> assertEquals(first.getLocation(), second.getLocation(), "Location doesn't match"),
                () -> assertMatch(first.getProjectId(), second.getProjectId(), "Project ID does not match"),
                () -> assertEquals(first.getElevationUnits(), second.getElevationUnits(), "Elevation units do not match"),
                () -> assertMatch(first.getChamberType(), second.getChamberType()),
                () -> assertEquals(first.getLengthUnits(), second.getLengthUnits(), "Length units do not match"),
                () -> {
                    // if the lock to match is from the database, the warning levels are calculated based on the location levels
                    if (fromDB) {
                        if (first.getHighWaterLowerPoolLocationLevel() != null) {
                            assertEquals((first.getHighWaterLowerPoolLocationLevel().getLevelValue() - first.getHighWaterLowerPoolWarningLevel()),
                                    second.getHighWaterLowerPoolWarningLevel(), DEFAULT_DELTA);
                        }
                        if (first.getHighWaterUpperPoolLocationLevel() != null) {
                            assertEquals((first.getHighWaterUpperPoolLocationLevel().getLevelValue() - first.getHighWaterUpperPoolWarningLevel()),
                                    second.getHighWaterUpperPoolWarningLevel(), DEFAULT_DELTA);
                        }
                    } else {
                        if (first.getHighWaterLowerPoolLocationLevel() != null) {
                            assertEquals(first.getHighWaterLowerPoolWarningLevel(), second.getHighWaterLowerPoolWarningLevel(), DEFAULT_DELTA);
                        }
                        if (first.getHighWaterUpperPoolLocationLevel() != null)
                        {
                            assertEquals(first.getHighWaterUpperPoolWarningLevel(), second.getHighWaterUpperPoolWarningLevel(), DEFAULT_DELTA);
                        }
                    }
                },
                () -> assertEquals(first.getLockLength(), second.getLockLength(), DEFAULT_DELTA, "Lock length does not match"),
                () -> assertEquals(first.getLockWidth(), second.getLockWidth(), DEFAULT_DELTA, "Lock width does not match"),
                () -> assertEquals(first.getNormalLockLift(), second.getNormalLockLift(), DEFAULT_DELTA, "Normal lock lift values do not match"),
                () -> assertEquals(first.getMaximumLockLift(), second.getMaximumLockLift(), DEFAULT_DELTA, "Maximum lock lift values do not match"),
                () -> assertEquals(first.getMinimumDraft(), second.getMinimumDraft(), DEFAULT_DELTA, "Minimum draft does not match"),
                () -> assertEquals(first.getVolumePerLockage(), second.getVolumePerLockage(), DEFAULT_DELTA, "Volume per lockage does not match"),
                () -> assertEquals(first.getVolumeUnits(), second.getVolumeUnits(), "Volume units does not match"),
                () -> assertMatch(first.getHighWaterLowerPoolLocationLevel(), second.getHighWaterLowerPoolLocationLevel()),
                () -> assertMatch(first.getHighWaterUpperPoolLocationLevel(), second.getHighWaterUpperPoolLocationLevel()),
                () -> assertMatch(first.getLowWaterLowerPoolLocationLevel(), second.getLowWaterLowerPoolLocationLevel()),
                () -> assertMatch(first.getLowWaterUpperPoolLocationLevel(), second.getLowWaterUpperPoolLocationLevel())
        );
    }

    public static void assertMatch(LockLocationLevelRef first, LockLocationLevelRef second)
    {
        if (first == null && second == null)
        {
            return;
        } else if (first == null || second == null)
        {
            fail("One of the LockLocationLevelRef is null");
        }
        assertAll(
                () -> assertEquals(first.getLevelLink(), second.getLevelLink(), "Level link does not match"),
                () -> assertEquals(first.getOfficeId(), second.getOfficeId(), "Office ID does not match"),
                () -> assertEquals(first.getLevelValue(), second.getLevelValue(), DEFAULT_DELTA, "Level value does not match"),
                () -> assertEquals(first.getLevelId(), second.getLevelId(), "Level IDs do not match"),
                () -> assertEquals(first.getSpecifiedLevelId(), second.getSpecifiedLevelId(), "Specified level IDs do not match")
        );
    }

    public static void assertMatch(TimeSeriesExtents first, TimeSeriesExtents second) {
        assertAll(
            () -> assertEquals(first.getLastUpdate(), second.getLastUpdate(), "Last Update time does not match"),
            () -> assertEquals(first.getVersionTime(), second.getVersionTime(), "Version time does not match"),
            () -> assertEquals(first.getEarliestTime(), second.getEarliestTime(), "Earliest time does not match"),
            () -> assertEquals(first.getLatestTime(), second.getLatestTime(), "Latest time does not match")
        );
    }

    public static void assertMatch(TimeExtents first, TimeExtents second) {
        assertAll(
            () -> assertEquals(first.getEarliestTime(), second.getEarliestTime(), "Start time does not match"),
            () -> assertEquals(first.getLatestTime(), second.getLatestTime(), "End time does not match")
        );
    }

    public static void assertMatch(CwmsIdTimeExtentsEntry first, CwmsIdTimeExtentsEntry second) {
        assertAll(
            () -> assertMatch(first.getId(), second.getId()),
            () -> assertMatch(first.getTimeExtents(), second.getTimeExtents())
        );
    }

    @FunctionalInterface
    public interface AssertMatchMethod<T>{
        void assertMatch(T first, T second);
    }
}
