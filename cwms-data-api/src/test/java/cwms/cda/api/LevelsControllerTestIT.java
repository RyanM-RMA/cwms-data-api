/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
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

package cwms.cda.api;

import cwms.cda.data.dao.LocationLevelsDaoImpl;
import cwms.cda.data.dto.LocationLevel;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;

import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.servlet.http.HttpServletResponse;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import static cwms.cda.api.Controllers.*;
import static helpers.FloatCloseTo.floatCloseTo;
import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
public class LevelsControllerTestIT extends DataApiTestIT {

    public static final String OFFICE = "SPK";
    private final List<LocationLevel> levelList = new ArrayList<>();

    @AfterEach
    void cleanup() throws Exception {
        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
            DSLContext dsl = dslContext(c, OFFICE);
            LocationLevelsDaoImpl dao = new LocationLevelsDaoImpl(dsl);
            for (LocationLevel level : levelList) {
                dao.deleteLocationLevel(level.getLocationLevelId(), level.getLevelDate(), level.getOfficeId(), false);
            }
        });
    }

    @Test
    void test_location_level() throws Exception {
        createLocation("level_as_single_value", true, OFFICE);
        String levelId = "level_as_single_value.Stor.Ave.1Day.Regulating";
        ZonedDateTime time = ZonedDateTime.of(2023, 6, 1, 0, 0, 0, 0, ZoneId.of("America/Los_Angeles"));
        LocationLevel level = new LocationLevel.Builder(levelId, time)
                .withOfficeId(OFFICE)
                .withConstantValue(1.0)
                .withLevelUnitsId("ac-ft")
                .build();
        levelList.add(level);
        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
            DSLContext dsl = dslContext(c, OFFICE);
            LocationLevelsDaoImpl dao = new LocationLevelsDaoImpl(dsl);
            dao.storeLocationLevel(level);
        });

        //Read level without unit
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam(EFFECTIVE_DATE, time.toInstant().toString())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/levels/{level-id}", levelId)
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("level-units-id", equalTo("m3"))
            // I think we need to create a custom matcher.
            // This really shouldn't use equals but due to a quirk in
            // RestAssured it appears to be necessary.
            .body("constant-value", equalTo(1233.4818f)); // 1 ac-ft to m3

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam(EFFECTIVE_DATE, time.toInstant().toString())
            .queryParam(UNIT, "ac-ft")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/levels/{level-id}", levelId)
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("level-units-id",equalTo("ac-ft"))
            .body("constant-value",equalTo(1.0F));
    }

    @Test
    void test_retrieve_time_window() throws Exception {
        createLocation("level_get_all_loc_1", true, OFFICE);
        String levelId = "level_get_all_loc_1.Flow.Ave.1Day.Regulating";
        ZonedDateTime time = ZonedDateTime.of(2023, 6, 1, 0, 0, 0, 0, ZoneId.of("America/Los_Angeles"));
        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
            LocationLevel level = new LocationLevel.Builder(levelId, time)
                    .withOfficeId(OFFICE)
                    .withConstantValue(1.0)
                    .withLevelUnitsId("cms")
                    .build();
            levelList.add(level);
            DSLContext dsl = dslContext(c, OFFICE);
            LocationLevelsDaoImpl dao = new LocationLevelsDaoImpl(dsl);
            dao.storeLocationLevel(level);
        });

        String locId2 = "level_get_all_loc_2";
        String levelId2 = locId2 + ".Stor.Ave.1Day.Regulating";
        createLocation(locId2, true, OFFICE);
        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {

            LocationLevel level = new LocationLevel.Builder(levelId2, time)
                    .withOfficeId(OFFICE)
                    .withConstantValue(2.0)
                    .withLevelUnitsId("ac-ft")
                    .build();
            levelList.add(level);
            DSLContext dsl = dslContext(c, OFFICE);
            LocationLevelsDaoImpl dao = new LocationLevelsDaoImpl(dsl);
            dao.storeLocationLevel(level);
        });

        //Read level with begin
        ExtractableResponse<Response> response = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(LEVEL_ID_MASK, "level_get_all_loc_*")
            .queryParam(BEGIN, "2020-06-01T00:00:00Z")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/levels/")
        .then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .extract();

        assertThat(response.path("levels.size()"),is(2));
        assertEquals(OFFICE, response.path("levels[0].office-id"));
        assertEquals(levelId, response.path("levels[0].location-level-id"));
        assertEquals("Regulating", response.path("levels[0].specified-level-id"));
        assertEquals("Ave", response.path("levels[0].parameter-type-id"));
        assertEquals("Flow", response.path("levels[0].parameter-id"));
        assertEquals("cms", response.path("levels[0].level-units-id"));
        assertEquals("2023-06-01T07:00:00Z", response.path("levels[0].level-date"));
        assertEquals("1Day", response.path("levels[0].duration-id"));
        assertEquals(OFFICE, response.path("levels[1].office-id"));
        assertEquals(levelId2, response.path("levels[1].location-level-id"));
        assertEquals("Regulating", response.path("levels[1].specified-level-id"));
        assertEquals("Ave", response.path("levels[1].parameter-type-id"));
        assertEquals("Stor", response.path("levels[1].parameter-id"));
        assertEquals("m3", response.path("levels[1].level-units-id"));
        assertEquals("2023-06-01T07:00:00Z", response.path("levels[1].level-date"));
        assertEquals("1Day", response.path("levels[1].duration-id"));

        //Read level without begin and end
        response = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(LEVEL_ID_MASK, "level_get_all_loc_*")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/levels/")
        .then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .extract();

        assertThat(response.path("levels.size()"),is(2));
        assertEquals(OFFICE, response.path("levels[0].office-id"));
        assertEquals(levelId, response.path("levels[0].location-level-id"));
        assertEquals("Regulating", response.path("levels[0].specified-level-id"));
        assertEquals("Ave", response.path("levels[0].parameter-type-id"));
        assertEquals("Flow", response.path("levels[0].parameter-id"));
        assertEquals("cms", response.path("levels[0].level-units-id"));
        assertEquals("2023-06-01T07:00:00Z", response.path("levels[0].level-date"));
        assertEquals("1Day", response.path("levels[0].duration-id"));
        assertEquals(OFFICE, response.path("levels[1].office-id"));
        assertEquals(levelId2, response.path("levels[1].location-level-id"));
        assertEquals("Regulating", response.path("levels[1].specified-level-id"));
        assertEquals("Ave", response.path("levels[1].parameter-type-id"));
        assertEquals("Stor", response.path("levels[1].parameter-id"));
        assertEquals("m3", response.path("levels[1].level-units-id"));
        assertEquals("2023-06-01T07:00:00Z", response.path("levels[1].level-date"));
        assertEquals("1Day", response.path("levels[1].duration-id"));
    }

    @Test
    void test_level_as_timeseries() throws Exception {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        createLocation("level_as_timeseries", true, OFFICE);
        String levelId = "level_as_timeseries.Flow.Ave.1Day.Regulating";
        ZonedDateTime time = ZonedDateTime.of(2023, 6, 1, 0, 0, 0, 0, ZoneId.of("America/Los_Angeles"));
        int effectiveDateCount = 10;
        NavigableMap<Instant, LocationLevel> levels = new TreeMap<>();
        for (int i = 0; i < effectiveDateCount; i++) {
            LocationLevel level = new LocationLevel.Builder(levelId, time.plusDays(i))
                    .withOfficeId(OFFICE)
                    .withConstantValue((double) i)
                    .withLevelUnitsId("cfs")
                    .build();
            levelList.add(level);
            levels.put(level.getLevelDate().toInstant(), level);
            CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
                DSLContext dsl = dslContext(c, OFFICE);
                LocationLevelsDaoImpl dao = new LocationLevelsDaoImpl(dsl);
                dao.storeLocationLevel(level);
            });
        }

        //Read level timeseries
        TimeSeries timeSeries =
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(BEGIN, time.toInstant().toString())
                .queryParam(END, time.plusDays(effectiveDateCount).toInstant().toString())
                .queryParam(INTERVAL, "1Hour")
                .queryParam(UNIT, "cfs")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/levels/" + levelId + "/timeseries/")
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_OK))
            .extract()
                .response()
                .as(TimeSeries.class);
        assertEquals("level_as_timeseries.Flow.Ave.1Hour.1Day.Regulating", timeSeries.getName());
        assertEquals(OFFICE, timeSeries.getOfficeId());
        assertEquals(time.toInstant(), timeSeries.getBegin().toInstant());
        assertEquals(time.plusDays(effectiveDateCount).toInstant(), timeSeries.getEnd().toInstant());
        assertEquals(24 * effectiveDateCount + 1, timeSeries.getTotal());
        List<TimeSeries.Record> values = timeSeries.getValues();
        for (int i = 0; i < values.size(); i++) {
            TimeSeries.Record tsrec = values.get(i);
            assertEquals(time.plusHours(i).toInstant(), tsrec.getDateTime().toInstant(), "Time check failed at iteration: " + i);
            assertEquals(0, tsrec.getQualityCode(), "Quality check failed at iteration: " + i);
            Double constantValue = levels.floorEntry(tsrec.getDateTime().toInstant())
                    .getValue()
                    .getConstantValue();
            assertEquals(constantValue, tsrec.getValue(), 0.0001, "Value check failed at iteration: " + i);
        }
    }


    @Test
    void test_get_all_location_level() throws Exception {
        String locId = "level_get_all_loc1";
        String levelId = locId + ".Stor.Ave.1Day.Regulating";
        createLocation(locId, true, OFFICE);
        final ZonedDateTime time = ZonedDateTime.of(2023, 6, 1, 0, 0, 0, 0, ZoneId.of("America"
                + "/Los_Angeles"));
        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
            LocationLevel level = new LocationLevel.Builder(levelId, time)
                    .withOfficeId(OFFICE)
                    .withConstantValue(1.0)
                    .withLevelUnitsId("ac-ft")
                    .build();
            levelList.add(level);
            DSLContext dsl = dslContext(c, OFFICE);
            LocationLevelsDaoImpl dao = new LocationLevelsDaoImpl(dsl);
            dao.storeLocationLevel(level);
        });

        String locId2 = "level_get_all_loc2";
        String levelId2 = locId2 + ".Stor.Ave.1Day.Regulating";
        createLocation(locId2, true, OFFICE);
        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {

            LocationLevel level = new LocationLevel.Builder(levelId2, time)
                    .withOfficeId(OFFICE)
                    .withConstantValue(2.0)
                    .withLevelUnitsId("ac-ft")
                    .build();
            levelList.add(level);
            DSLContext dsl = dslContext(c, OFFICE);
            LocationLevelsDaoImpl dao = new LocationLevelsDaoImpl(dsl);
            dao.storeLocationLevel(level);
        });

        String startStr = "2023-06-01T00:00:00Z";
        String endStr = "2023-06-02T00:00:00Z";

        //Read level without unit
        ExtractableResponse<Response> response = given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(LEVEL_ID_MASK, "level_get_all.*")
                .queryParam(BEGIN, startStr)
                .queryParam(END, endStr)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/levels/")
            .then()
            .assertThat()
                .log().ifValidationFails(LogDetail.ALL, true)
                .statusCode(is(HttpServletResponse.SC_OK))
            .extract();

                assertThat(response.path("levels.size()"),is(2));

                assertThat(response.path("levels[0].office-id"),equalTo(OFFICE));
                assertThat(response.path("levels[0].location-level-id"),equalTo(levelId));
                assertThat(response.path("levels[0].specified-level-id"),equalTo("Regulating"));
                assertThat(response.path("levels[0].parameter-type-id"),equalTo("Ave"));
                assertThat(response.path("levels[0].parameter-id"),equalTo("Stor"));
                assertThat(response.path("levels[0].level-units-id"),equalTo("m3"));
                assertThat(response.path("levels[0].level-date"),equalTo("2023-06-01T07:00:00Z"));
                assertThat(response.path("levels[0].duration-id"),equalTo("1Day"));
                double actual0 = Float.valueOf((float) response.path("levels[0].constant-value")).doubleValue();
                assertThat(actual0, closeTo(1233.0, 10.0));

                assertThat(response.path("levels[1].office-id"),equalTo(OFFICE));
                assertThat(response.path("levels[1].location-level-id"),equalTo(levelId2));
                assertThat(response.path("levels[1].specified-level-id"),equalTo("Regulating"));
                assertThat(response.path("levels[1].parameter-type-id"),equalTo("Ave"));
                assertThat(response.path("levels[1].parameter-id"),equalTo("Stor"));
                assertThat(response.path("levels[1].level-units-id"),equalTo("m3"));
                assertThat(response.path("levels[1].level-date"),equalTo("2023-06-01T07:00:00Z"));
                assertThat(response.path("levels[1].duration-id"),equalTo("1Day"));
                double actual1 = Float.valueOf((float) response.path("levels[1].constant-value")).doubleValue();
                assertThat(actual1, closeTo(2466.9636f, 1.0));

        response = given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(UNIT, "SI")
                .queryParam(LEVEL_ID_MASK, "level_get_all.*")
                .queryParam(BEGIN, startStr)
                .queryParam(END, endStr)
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/levels/")
                .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL, true)
                .statusCode(is(HttpServletResponse.SC_OK))
                .extract();
                assertThat(response.path("levels.size()"),is(2));

                assertThat(response.path("levels[0].office-id"),equalTo(OFFICE));
                assertThat(response.path("levels[0].location-level-id"),equalTo(levelId));
                assertThat(response.path("levels[0].specified-level-id"),equalTo("Regulating"));
                assertThat(response.path("levels[0].parameter-type-id"),equalTo("Ave"));
                assertThat(response.path("levels[0].parameter-id"),equalTo("Stor"));
                assertThat(response.path("levels[0].level-units-id"),equalTo("m3"));
                assertThat(response.path("levels[0].level-date"),equalTo("2023-06-01T07:00:00Z"));
                assertThat(response.path("levels[0].duration-id"),equalTo("1Day"));
                actual0 = Float.valueOf((float) response.path("levels[0].constant-value")).doubleValue();
                assertThat(actual0, closeTo(1233.4818, 1.0));

                assertThat(response.path("levels[1].office-id"),equalTo(OFFICE));
                assertThat(response.path("levels[1].location-level-id"),equalTo(levelId2));
                assertThat(response.path("levels[1].specified-level-id"),equalTo("Regulating"));
                assertThat(response.path("levels[1].parameter-type-id"),equalTo("Ave"));
                assertThat(response.path("levels[1].parameter-id"),equalTo("Stor"));
                assertThat(response.path("levels[1].level-units-id"),equalTo("m3"));
                assertThat(response.path("levels[1].level-date"),equalTo("2023-06-01T07:00:00Z"));
                assertThat(response.path("levels[1].duration-id"),equalTo("1Day"));
                actual1 = Float.valueOf((float) response.path("levels[1].constant-value")).doubleValue();
                assertThat(actual1, closeTo(2466.9636, 1.0));

        response = given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(UNIT, "EN")
                .queryParam(LEVEL_ID_MASK, "level_get_all.*")
                .queryParam(BEGIN, startStr)
                .queryParam(END, endStr)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/levels/")
            .then()
            .assertThat()
                .log().ifValidationFails(LogDetail.ALL, true)
                .statusCode(is(HttpServletResponse.SC_OK))
                .extract();
                assertThat(response.path("levels.size()"),is(2));

                assertThat(response.path("levels[0].office-id"),equalTo(OFFICE));
                assertThat(response.path("levels[0].location-level-id"),equalTo(levelId));
                assertThat(response.path("levels[0].specified-level-id"),equalTo("Regulating"));
                assertThat(response.path("levels[0].parameter-type-id"),equalTo("Ave"));
                assertThat(response.path("levels[0].parameter-id"),equalTo("Stor"));
                assertThat(response.path("levels[0].level-units-id"),equalTo("ac-ft"));
                assertThat(response.path("levels[0].level-date"),equalTo("2023-06-01T07:00:00Z"));
                assertThat(response.path("levels[0].duration-id"),equalTo("1Day"));
                assertThat(response.path("levels[0].constant-value"), floatCloseTo(1.0, 0.01));

                assertThat(response.path("levels[1].office-id"),equalTo(OFFICE));
                assertThat(response.path("levels[1].location-level-id"),equalTo(levelId2));
                assertThat(response.path("levels[1].specified-level-id"),equalTo("Regulating"));
                assertThat(response.path("levels[1].parameter-type-id"),equalTo("Ave"));
                assertThat(response.path("levels[1].parameter-id"),equalTo("Stor"));
                assertThat(response.path("levels[1].level-units-id"),equalTo("ac-ft"));
                assertThat(response.path("levels[1].level-date"),equalTo("2023-06-01T07:00:00Z"));
                assertThat(response.path("levels[1].duration-id"),equalTo("1Day"));
                assertThat(response.path("levels[1].constant-value"), floatCloseTo(2.0, 0.01));
    }

    @Test
    void test_get_one_units() throws Exception {
        createLocation("level_as_single_value", true, OFFICE);
        String levelId = "level_as_single_value.Stor.Ave.1Day.Regulating";
        ZonedDateTime time = ZonedDateTime.of(2023, 6, 1, 0, 0, 0, 0, ZoneId.of("America/Los_Angeles"));
        LocationLevel level = new LocationLevel.Builder(levelId, time)
                .withOfficeId(OFFICE)
                .withConstantValue(1.0)
                .withLevelUnitsId("ac-ft")
                .build();
        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
            DSLContext dsl = dslContext(c, OFFICE);
            LocationLevelsDaoImpl dao = new LocationLevelsDaoImpl(dsl);
            dao.storeLocationLevel(level);
        });

        //Read level with unit
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(UNIT, "SI")
            .queryParam(EFFECTIVE_DATE, time.toInstant().toString())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/levels/{level-id}", levelId)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("level-units-id", equalTo("m3"))
            // I think we need to create a custom matcher.
            // This really shouldn't use equals but due to a quirk in
            // RestAssured it appears to be necessary.
            .body("constant-value", equalTo(1233.4818f)); // 1 ac-ft to m3

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam(EFFECTIVE_DATE, time.toInstant().toString())
            .queryParam(UNIT, "ac-ft")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/levels/{level-id}", levelId)
        .then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("level-units-id",equalTo("ac-ft"))
            .body("constant-value",equalTo(1.0F));

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam(EFFECTIVE_DATE, time.toInstant().toString())
            .queryParam(UNIT, "EN")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/levels/{level-id}", levelId)
        .then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("level-units-id",equalTo("ac-ft"))
            .body("constant-value",equalTo(1.0F));

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam(EFFECTIVE_DATE, time.toInstant().toString())
            .queryParam(UNIT, "ft3")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/levels/{level-id}", levelId)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("level-units-id",equalTo("ft3"))
            .body("constant-value",equalTo(43560.0F));
    }

    @Test
    void test_get_all_earliest_time() throws Exception {
        String locId = "level_get_all_loc1";
        String levelId = locId + ".Stor.Ave.1Day.Regulating";
        createLocation(locId, true, OFFICE);
        final ZonedDateTime time = ZonedDateTime.of(2023, 6, 1, 0, 0, 0, 0, ZoneId.of("America"
                + "/Los_Angeles"));
        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
            LocationLevel level = new LocationLevel.Builder(levelId, time)
                    .withOfficeId(OFFICE)
                    .withConstantValue(1.0)
                    .withLevelUnitsId("ac-ft")
                    .build();
            levelList.add(level);
            DSLContext dsl = dslContext(c, OFFICE);
            LocationLevelsDaoImpl dao = new LocationLevelsDaoImpl(dsl);
            dao.storeLocationLevel(level);
        });

        String locId2 = "level_get_all_loc2";
        String levelId2 = locId2 + ".Stor.Ave.1Day.Regulating";
        createLocation(locId2, true, OFFICE);
        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {

            LocationLevel level = new LocationLevel.Builder(levelId2, time)
                    .withOfficeId(OFFICE)
                    .withConstantValue(2.0)
                    .withLevelUnitsId("ac-ft")
                    .build();
            levelList.add(level);
            DSLContext dsl = dslContext(c, OFFICE);
            LocationLevelsDaoImpl dao = new LocationLevelsDaoImpl(dsl);
            dao.storeLocationLevel(level);
        });

        // Get all with minimum timestamp accepted by the database
        ExtractableResponse<Response> response = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(LEVEL_ID_MASK, "level_get_all.*")
            .queryParam(BEGIN, "-4712-11-25T00:00:00")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/levels/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .extract();

        assertThat(response.path("levels.size()"),is(2));

        assertThat(response.path("levels[0].office-id"),equalTo(OFFICE));
        assertThat(response.path("levels[0].location-level-id"),equalTo(levelId));
        assertThat(response.path("levels[0].specified-level-id"),equalTo("Regulating"));
        assertThat(response.path("levels[0].parameter-type-id"),equalTo("Ave"));
        assertThat(response.path("levels[0].parameter-id"),equalTo("Stor"));
        assertThat(response.path("levels[0].level-units-id"),equalTo("m3"));
        assertThat(response.path("levels[0].level-date"),equalTo("2023-06-01T07:00:00Z"));
        assertThat(response.path("levels[0].duration-id"),equalTo("1Day"));
        double actual0 = Float.valueOf((float) response.path("levels[0].constant-value")).doubleValue();
        assertThat(actual0, closeTo(1233.0, 10.0));

        assertThat(response.path("levels[1].office-id"),equalTo(OFFICE));
        assertThat(response.path("levels[1].location-level-id"),equalTo(levelId2));
        assertThat(response.path("levels[1].specified-level-id"),equalTo("Regulating"));
        assertThat(response.path("levels[1].parameter-type-id"),equalTo("Ave"));
        assertThat(response.path("levels[1].parameter-id"),equalTo("Stor"));
        assertThat(response.path("levels[1].level-units-id"),equalTo("m3"));
        assertThat(response.path("levels[1].level-date"),equalTo("2023-06-01T07:00:00Z"));
        assertThat(response.path("levels[1].duration-id"),equalTo("1Day"));
        double actual1 = Float.valueOf((float) response.path("levels[1].constant-value")).doubleValue();
        assertThat(actual1, closeTo(2466.9636f, 1.0));

        //Read level without time window
        response = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(LEVEL_ID_MASK, "level_get_all.*")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/levels/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .extract();

        assertThat(response.path("levels.size()"),is(2));

        assertThat(response.path("levels[0].office-id"),equalTo(OFFICE));
        assertThat(response.path("levels[0].location-level-id"),equalTo(levelId));
        assertThat(response.path("levels[0].specified-level-id"),equalTo("Regulating"));
        assertThat(response.path("levels[0].parameter-type-id"),equalTo("Ave"));
        assertThat(response.path("levels[0].parameter-id"),equalTo("Stor"));
        assertThat(response.path("levels[0].level-units-id"),equalTo("m3"));
        assertThat(response.path("levels[0].level-date"),equalTo("2023-06-01T07:00:00Z"));
        assertThat(response.path("levels[0].duration-id"),equalTo("1Day"));
        actual0 = Float.valueOf((float) response.path("levels[0].constant-value")).doubleValue();
        assertThat(actual0, closeTo(1233.0, 10.0));

        assertThat(response.path("levels[1].office-id"),equalTo(OFFICE));
        assertThat(response.path("levels[1].location-level-id"),equalTo(levelId2));
        assertThat(response.path("levels[1].specified-level-id"),equalTo("Regulating"));
        assertThat(response.path("levels[1].parameter-type-id"),equalTo("Ave"));
        assertThat(response.path("levels[1].parameter-id"),equalTo("Stor"));
        assertThat(response.path("levels[1].level-units-id"),equalTo("m3"));
        assertThat(response.path("levels[1].level-date"),equalTo("2023-06-01T07:00:00Z"));
        assertThat(response.path("levels[1].duration-id"),equalTo("1Day"));
        actual1 = Float.valueOf((float) response.path("levels[1].constant-value")).doubleValue();
        assertThat(actual1, closeTo(2466.9636f, 1.0));
    }

    @Test
    void testRetrievalInvalidLevelName()
    {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(EFFECTIVE_DATE, "2023-06-01T00:00:00Z")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/levels/invalid.level_name")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST));
    }

    @ParameterizedTest
    @EnumSource(GetAllTestNewAliases.class)
    void test_get_all_aliases_new(GetAllTestNewAliases test)
    {
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(test._accept)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(LEVEL_ID_MASK, "level_get_all.*")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/levels/")
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(is(test._expectedContentType))
                .statusCode(is(HttpServletResponse.SC_OK));
    }

    @ParameterizedTest
    @EnumSource(GetAllTestLegacy.class)
    void test_get_all_aliases_legacy(GetAllTestLegacy test)
    {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(FORMAT, test._format)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(LEVEL_ID_MASK, "level_get_all.*")
        .when()
            .redirects()
            .follow(true)
            .redirects()
            .max(3)
            .get("/levels/")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .contentType(is(test._expectedContentType));
    }

    enum GetAllTestLegacy
    {
        JSON(Formats.JSON_LEGACY, Formats.JSON),
        XML(Formats.XML_LEGACY, Formats.XML),
        TAB(Formats.TAB_LEGACY, Formats.TAB),
        CSV(Formats.CSV_LEGACY, Formats.CSV),
        ;
        final String _format;
        final String _expectedContentType;

        GetAllTestLegacy(String format, String expectedContentType)
        {
            _format = format;
            _expectedContentType = expectedContentType;
        }
    }

    enum GetAllTestNewAliases
    {
        DEFAULT(Formats.DEFAULT, Formats.JSONV2),
        JSON(Formats.JSON, Formats.JSONV2),
        JSONV1(Formats.JSONV1, Formats.JSONV1),
        JSONV2(Formats.JSONV2, Formats.JSONV2),
        ;
        final String _accept;
        final String _expectedContentType;

        GetAllTestNewAliases(String accept, String expectedContentType)
        {
            _accept = accept;
            _expectedContentType = expectedContentType;
        }
    }
}
