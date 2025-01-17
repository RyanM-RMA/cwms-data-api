/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileDao;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import javax.servlet.http.HttpServletResponse;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
final class TimeSeriesProfileControllerIT extends DataApiTestIT {
    private static final Logger LOGGER = Logger.getLogger(TimeSeriesProfileControllerIT.class.getName());
    private static final String OFFICE_ID = "SPK";
    private static final TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
    private final InputStream resource = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile.json");
    private final InputStream resource2 = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile_2.json");
    private final InputStream resource3 = this.getClass()
            .getResourceAsStream("/cwms/cda/api/timeseriesprofile/ts_profile_3.json");
    private String tsData;
    private String tsData2;
    private String tsData3;
    private TimeSeriesProfile tsProfile;
    private TimeSeriesProfile tsProfile2;
    private TimeSeriesProfile tsProfile3;

    @BeforeEach
    public void setup() throws Exception {
        assertNotNull(resource);
        assertNotNull(resource2);
        assertNotNull(resource3);
        tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        tsData2 = IOUtils.toString(resource2, StandardCharsets.UTF_8);
        tsData3 = IOUtils.toString(resource3, StandardCharsets.UTF_8);
        tsProfile = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                TimeSeriesProfile.class), tsData, TimeSeriesProfile.class);
        tsProfile2 = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                TimeSeriesProfile.class), tsData2, TimeSeriesProfile.class);
        tsProfile3 = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                TimeSeriesProfile.class), tsData3, TimeSeriesProfile.class);
        createLocation(tsProfile.getLocationId().getName(), true, OFFICE_ID, "SITE");
        createLocation(tsProfile2.getLocationId().getName(), true, OFFICE_ID, "SITE");
        createLocation(tsProfile3.getLocationId().getName(), true, OFFICE_ID, "SITE");
    }

    @AfterEach
    public void tearDown() throws Exception {
        // cleans up time series profiles between tests
        // this is necessary because the tests reuse the same profiles
        cleanupTS(tsProfile.getLocationId().getName(), tsProfile.getKeyParameter());
        cleanupTS(tsProfile2.getLocationId().getName(), tsProfile2.getKeyParameter());
        cleanupTS(tsProfile3.getLocationId().getName(), tsProfile3.getKeyParameter());
    }

    @Test
    void test_create_retrieve_TimeSeriesProfile() {
        // Create a new TimeSeriesProfile
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tsData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve the TimeSeriesProfile
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(LOCATION_ID, tsProfile.getLocationId().getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile/" + tsProfile.getLocationId().getName() + "/" + tsProfile.getKeyParameter())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("location-id.name", is(tsProfile.getLocationId().getName()))
            .body("key-parameter", is(tsProfile.getKeyParameter()))
            .body("location-id.office-id", is(tsProfile.getLocationId().getOfficeId()))
            .body("description", is(tsProfile.getDescription()))
            .body("parameter-list[0]", equalTo(tsProfile.getKeyParameter()))
        ;
    }

    @Test
    void test_store_retrieve_with_ref_TS() throws Exception {
        // create a new TimeSeries to reference
        createTimeseries(OFFICE_ID, tsProfile3.getReferenceTsId().getName());

        // Create a new TimeSeriesProfile
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tsData3)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;
        // Retrieve the TimeSeriesProfile
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(LOCATION_ID, tsProfile3.getLocationId().getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile/" + tsProfile3.getLocationId().getName() + "/" + tsProfile3.getKeyParameter())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("location-id.name", is(tsProfile3.getLocationId().getName()))
            .body("key-parameter", is(tsProfile3.getKeyParameter()))
            .body("location-id.office-id", is(tsProfile3.getLocationId().getOfficeId()))
            .body("description", is(tsProfile3.getDescription()))
            .body("parameter-list[1]", equalTo(tsProfile3.getKeyParameter()))
            .body("reference-ts-id.name", is(tsProfile3.getReferenceTsId().getName()))
        ;
    }

    @Test
    void test_get_all_TimeSeriesProfilePaginated() {
        // Create a new TimeSeriesProfile
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tsData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;
        // Create a new TimeSeriesProfile
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tsData2)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve all TimeSeriesProfiles
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("profile-list.size()", is(2))
        ;

        // Retrieve TimeSeriesProfiles with pagination, page 1
        ExtractableResponse<Response> response = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(PAGE_SIZE, 1)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("profile-list.size()", is(1))
            .body("next-page", is(notNullValue())).extract()
        ;

        String nextPageCursor = response.path("next-page");
        assert nextPageCursor != null;

        // Retrieve TimeSeriesProfiles with pagination, page 2
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(PAGE_SIZE, 1)
            .queryParam(PAGE, nextPageCursor)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("profile-list.size()", is(1))
        ;
    }

    @Test
    void test_get_all_TimeSeriesProfilePaginated_minimum() {
        // Create a new TimeSeriesProfile
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tsData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve all TimeSeriesProfiles
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("profile-list.size()", is(1))
        ;

        // Retrieve TimeSeriesProfiles with pagination, page 1, assert that next-page is null
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(PAGE_SIZE, 1)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("profile-list.size()", is(1))
            .body("next-page", is(nullValue()))
        ;
    }

    @Test
    void test_get_all_TimeSeriesProfile() {
        // Create a new TimeSeriesProfile
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tsData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Create a new TimeSeriesProfile
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tsData2)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Retrieve all TimeSeriesProfiles
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("profile-list.size()", is(2))
        ;
    }

    @Test
    void test_delete_TimeSeriesProfile() {

        // Create the Time Series Profile
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tsData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/profile/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Delete the Time Series Profile
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tsData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/profile/" + tsProfile.getLocationId().getName() + "/" + tsProfile.getKeyParameter())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Retrieve the Time Series Profile and assert that it is not found
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(LOCATION_ID, tsProfile.getLocationId().getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/profile/" + tsProfile.getKeyParameter())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_delete_nonExistent_TimeSeriesProfile() {
        // Delete the Time Series Profile
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .body(tsData)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(LOCATION_ID, "nonexistent")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/profile/" + tsProfile.getLocationId().getName() + "/" + tsProfile.getKeyParameter())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    private void cleanupTS(String locationId, String keyParameter) throws Exception {
        try {
            CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
            db.connection(c -> {
                DSLContext dsl = dslContext(c, OFFICE_ID);
                TimeSeriesProfileDao dao = new TimeSeriesProfileDao(dsl);
                dao.deleteTimeSeriesProfile(locationId, keyParameter, OFFICE_ID);
            });
        } catch (NotFoundException e) {
            LOGGER.log(Level.CONFIG, "Unable to cleanup TS Profile - not found", e);
        }
    }
}
