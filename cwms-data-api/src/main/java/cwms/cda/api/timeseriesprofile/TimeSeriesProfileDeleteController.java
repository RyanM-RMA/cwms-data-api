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

package cwms.cda.api.timeseriesprofile;

import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.LOCATION_ID;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PARAMETER_ID;
import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.api.Controllers.STATUS_400;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileDao;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public final class TimeSeriesProfileDeleteController extends TimeSeriesProfileBase implements Handler {

    public TimeSeriesProfileDeleteController(MetricRegistry metrics) {
        tspMetrics(metrics);
    }

    @OpenApi(
            queryParams = {
                @OpenApiParam(name = OFFICE, description = "The office associated with the time series profile"),
            },
            pathParams = {
                @OpenApiParam(name = LOCATION_ID, description = "The location ID associated with the time "
                    + "series profile"),
                @OpenApiParam(name = PARAMETER_ID, description = "The key parameter associated with the "
                    + "time series profile")
            },
            method = HttpMethod.DELETE,
            summary = "Delete a time series profile",
            tags = {TAG},
            responses = {
                @OpenApiResponse(status = STATUS_400, description = "Invalid input"),
                @OpenApiResponse(status = STATUS_204, description = "Time series profile deleted"),
                @OpenApiResponse(status = STATUS_404, description = "Time series profile not found"),
                @OpenApiResponse(status = STATUS_501, description = "Internal server error")
            }
    )
    @Override
    public void handle(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            String keyParameter = ctx.pathParam(PARAMETER_ID);
            String office = requiredParam(ctx, OFFICE);
            String locationId = ctx.pathParam(LOCATION_ID);
            TimeSeriesProfileDao tspDao = getProfileDao(dsl);
            tspDao.deleteTimeSeriesProfile(locationId, keyParameter, office);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }
}
