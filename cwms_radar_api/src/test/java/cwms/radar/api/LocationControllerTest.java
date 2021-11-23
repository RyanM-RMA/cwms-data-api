package cwms.radar.api;

import cwms.radar.api.enums.Nation;
import cwms.radar.data.dto.Location;
import cwms.radar.formatters.Formats;
import cwms.radar.security.CwmsAuthException;
import cwms.radar.security.CwmsAuthorizer;
import cwms.radar.security.CwmsNoAuthorizer;
import fixtures.TestHttpServletResponse;
import fixtures.TestServletInputStream;
import io.javalin.http.Context;
import io.javalin.http.HandlerType;
import io.javalin.http.util.ContextUtil;
import io.javalin.plugin.json.JavalinJackson;
import io.javalin.plugin.json.JsonMapperKt;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.MetricRegistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LocationControllerTest extends ControllerTest
{
    private static final String OFFICE_ID = "LRL";

    @Test
    void testDeserializeLocationXml() throws IOException
    {
        String xml = loadResourceAsString("cwms/radar/api/location_create.xml");
        assertNotNull(xml);
        Location location = LocationController.deserializeLocation(xml, Formats.XML, OFFICE_ID);
        assertNotNull(location);
        assertEquals("LOC_TEST", location.getName());
        assertEquals("LRL", location.getOfficeId());
        assertEquals("NGVD-29", location.getHorizontalDatum());
        assertEquals("UTC", location.getTimezoneName());
        assertEquals(Nation.US, location.getNation());
    }

    @Test
    void testDeserializeLocationJSON() throws IOException
    {
        String json = loadResourceAsString("cwms/radar/api/location_create.json");
        assertNotNull(json);
        Location location = LocationController.deserializeLocation(json, Formats.JSON, OFFICE_ID);
        assertNotNull(location);
        assertEquals("LOC_TEST", location.getName());
        assertEquals("LRL", location.getOfficeId());
        assertEquals("NGVD-29", location.getHorizontalDatum());
        assertEquals("UTC", location.getTimezoneName());
        assertEquals(Nation.US, location.getNation());
    }

    /**
     * Test of getOne method, of class LocationController.
     */
    @Test
    public void test_basic_operations() throws Exception {
        final String testBody = "";
        LocationController instance = new LocationController(new MetricRegistry());
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = new TestHttpServletResponse();
        HashMap<String,Object> attributes = new HashMap<>();
        attributes.put(ContextUtil.maxRequestSizeKey,Integer.MAX_VALUE);
        attributes.put(JsonMapperKt.JSON_MAPPER_KEY,new JavalinJackson());
        attributes.put("Authorizer",new CwmsNoAuthorizer());
        when(request.getInputStream()).thenReturn(new TestServletInputStream(testBody));

        final Context context = ContextUtil.init(request,response,"*",new HashMap<String,String>(), HandlerType.GET,attributes);
        context.attribute("database",getTestConnection());

        when(request.getAttribute("database")).thenReturn(getTestConnection());

        assertNotNull( context.attribute("database"), "could not get the connection back as an attribute");
        System.out.println("getOne");
        String Location_id = "SimpleNoAlias";


        instance.getOne(context, Location_id);
        assertEquals(200,context.status(), "incorrect status code returned");

        assertThrows( CwmsAuthException.class , () -> {
            instance.create(context);
        });

        assertThrows( CwmsAuthException.class, () -> {
            instance.update(context, Location_id);
        });

        assertThrows( CwmsAuthException.class, () -> {
            instance.delete(context, Location_id);
        });

    }

}