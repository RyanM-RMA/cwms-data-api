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

import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.LOCATION_ID;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PARAMETER_ID;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileParserDao;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParser;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public final class TimeSeriesProfileParserController extends TimeSeriesProfileParserBase implements Handler {

    public TimeSeriesProfileParserController(MetricRegistry metrics) {
        tspMetrics(metrics);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "The office associated with the "
                    + "TimeSeriesProfile"),
        },
        pathParams = {
            @OpenApiParam(name = PARAMETER_ID, required = true, description = "The ID of the TimeSeriesProfileParser"),
            @OpenApiParam(name = LOCATION_ID, required = true, description = "The location ID associated"
                + " with the TimeSeriesProfile"),
        },
        responses = {
            @OpenApiResponse(status = STATUS_200,
                description = "A TimeSeriesProfileParser object",
                content = {
                    @OpenApiContent(from = TimeSeriesProfileParser.class, type = Formats.JSONV1),
                    @OpenApiContent(from = TimeSeriesProfileParser.class, type = Formats.JSON),
                }),
            @OpenApiResponse(status = STATUS_404, description = "The provided combination of parameters did not"
                + " find a TimeSeriesProfileParser object"),
            @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                + "implemented")

        },
        method = HttpMethod.GET,
        summary = "Get a TimeSeriesProfile Parser by ID",
        tags = {TAG}
    )

    @Override
    public void handle(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            String parameterId = ctx.pathParam(PARAMETER_ID);
            String officeId = requiredParam(ctx, OFFICE);
            String locationId = ctx.pathParam(LOCATION_ID);
            TimeSeriesProfileParserDao tspParserDao = getParserDao(dsl);
            TimeSeriesProfileParser tspParser = tspParserDao.retrieveTimeSeriesProfileParser(locationId,
                    parameterId, officeId);
            String acceptHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(acceptHeader, TimeSeriesProfileParser.class);
            String result = Formats.format(contentType, tspParser);
            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(result);
        }
    }

}
