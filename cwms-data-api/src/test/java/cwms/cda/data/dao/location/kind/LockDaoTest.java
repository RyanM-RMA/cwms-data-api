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

package cwms.cda.data.dao.location.kind;

import cwms.cda.api.enums.Nation;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.Lock;
import cwms.cda.data.dto.location.kind.LockLocationLevelRef;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DTOMatch;

import java.time.ZoneId;

import org.jooq.Field;
import org.jooq.Record3;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.udt.records.LOCK_OBJ_T;

final class LockDaoTest {

    @Test
    void testJooqTypeConversion() {
        Lock expected = buildTestLock();
        LOCK_OBJ_T lockObjT = LockDao.map(expected);
        Lock lock = LockDao.map(lockObjT);
        DTOMatch.assertMatch(expected, lock, false);
    }

    @Test
    void testSerializeDeserialize() {
        Lock expected = buildTestLock();
        ContentType contentType = new ContentType(Formats.JSONV1);
        String json = Formats.format(contentType, expected);
        Lock lock = Formats.parseContent(contentType, json, Lock.class);
        DTOMatch.assertMatch(expected, lock, false);
    }

    @Test
    void testSerializeDeserializeWithNullLevels()
    {
        Lock expected = buildTestLockWithNullLevels();
        ContentType contentType = new ContentType(Formats.JSONV1);
        String json = Formats.format(contentType, expected);
        Lock lock = Formats.parseContent(contentType, json, Lock.class);
        DTOMatch.assertMatch(expected, lock, false);
    }

    @Test
    void testJooqRecordConversion() {
        Field<String> officeField = DSL.field("DB_OFFICE_ID", String.class);
        Field<String> baseLocField = DSL.field("PROJECT_ID", String.class);
        Field<String> subLocField = DSL.field("LOCK_ID", String.class);
        Record3<String, String, String> lockRecord = DSL.using(SQLDialect.ORACLE18C)
            .newRecord(officeField,
                baseLocField,
                subLocField);
        lockRecord.setValue(officeField, "SPK");
        lockRecord.setValue(baseLocField, "BASE");
        lockRecord.setValue(subLocField, "SUB");
        CwmsId expected = CwmsId.buildCwmsId("SPK", "BASE-SUB");
        CwmsId cwmsId = LockDao.map(lockRecord);
        DTOMatch.assertMatch(expected, cwmsId);
    }

    private Lock buildTestLock() {
        return new Lock.Builder()
            .withLocation(buildTestLocation())
            .withProjectId(new CwmsId.Builder()
                .withName("PROJECT")
                .withOfficeId("LRD")
                .build())
            .withLockLength(100.0)
            .withMaximumLockLift(20.0)
            .withVolumeUnits("ft3")
            .withLengthUnits("ft")
            .withElevationUnits("ft")
            .withChamberType(new LookupType.Builder().withOfficeId("LRD").withActive(true)
                 .withDisplayValue("LOCK").withTooltip("Land Side Main").build())
            .withLockWidth(100.0)
            .withNormalLockLift(10.0)
            .withVolumePerLockage(100.0)
            .withMinimumDraft(5.0)
            .withChamberType(new LookupType.Builder().withOfficeId("LRD").withActive(true)
                .withTooltip("CHAMBER").withDisplayValue("Land Side Main").build())
            .withHighWaterLowerPoolLocationLevel(
                new LockLocationLevelRef("/locks/TEST_LOCATION2.Elev-Closure.Inst.0.High Water Lower Pool?office=LRL", 18.5))
            .withHighWaterUpperPoolLocationLevel(
                new LockLocationLevelRef("/locks/TEST_LOCATION2.Elev-Closure.Inst.0.High Water Upper Pool?office=LRL", 20.5))
            .withLowWaterLowerPoolLocationLevel(
                new LockLocationLevelRef("/locks/TEST_LOCATION2.Elev-Closure.Inst.0.Low Water Lower Pool?office=LRL", 31.14))
            .withLowWaterUpperPoolLocationLevel(
                new LockLocationLevelRef("/locks/TEST_LOCATION2.Elev-Closure.Inst.0.Low Water Upper Pool?office=LRL", 16.5))
            .withHighWaterLowerPoolWarningLevel(10.0)
            .withHighWaterUpperPoolWarningLevel(10.0)
            .build();
    }

    private Lock buildTestLockWithNullLevels() {
        return new Lock.Builder()
            .withLocation(buildTestLocation())
            .withProjectId(new CwmsId.Builder()
                .withName("PROJECT")
                .withOfficeId("LRD")
                .build())
            .withLockLength(100.0)
            .withMaximumLockLift(20.0)
            .withVolumeUnits("ft3")
            .withLengthUnits("ft")
            .withChamberType(new LookupType.Builder().withOfficeId("LRD").withActive(true)
                .withDisplayValue("LOCK").withTooltip("Land Side Main").build())
            .withLockWidth(100.0)
            .withNormalLockLift(10.0)
            .withVolumePerLockage(100.0)
            .withMinimumDraft(5.0)
            .withElevationUnits("ft")
            .withChamberType(new LookupType.Builder().withOfficeId("LRD").withActive(true)
                .withTooltip("CHAMBER").withDisplayValue("Land Side Main").build())
            .withHighWaterLowerPoolLocationLevel(null)
            .withHighWaterUpperPoolLocationLevel(null)
            .withLowWaterLowerPoolLocationLevel(null)
            .withLowWaterUpperPoolLocationLevel(null)
            .build();
    }

    private Location buildTestLocation() {
        return new Location.Builder("TEST_LOCATION2", "LOCK", ZoneId.of("UTC"),
            50.0, 50.0, "NVGD29", "LRL")
            .withElevation(10.0)
            .withElevationUnits("ft")
            .withLocationType("SITE")
            .withCountyName("Sacramento")
            .withNation(Nation.US)
            .withActive(true)
            .withStateInitial("CA")
            .withBoundingOfficeId("LRL")
            .withLongName("TEST_LOCATION")
            .withPublishedLatitude(50.0)
            .withPublishedLongitude(50.0)
            .withDescription("for testing")
            .withNearestCity("Davis")
            .build();
    }
}
