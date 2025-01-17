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

package cwms.cda.api.watersupply;

import static cwms.cda.api.Controllers.BEGIN;
import static cwms.cda.api.Controllers.CONTRACT_NAME;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.END_TIME_INCLUSIVE;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PROJECT_ID;
import static cwms.cda.api.Controllers.START;
import static cwms.cda.api.Controllers.START_TIME_INCLUSIVE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.TIMEZONE;
import static cwms.cda.api.Controllers.UNIT;
import static cwms.cda.api.Controllers.WATER_USER;
import static cwms.cda.api.Controllers.requiredInstant;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.watersupply.WaterContractDao;
import cwms.cda.data.dao.watersupply.WaterSupplyAccountingDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.watersupply.WaterSupplyAccounting;
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
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
import java.time.Instant;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public class AccountingCatalogController implements Handler {
    private static final Logger LOGGER = Logger.getLogger(AccountingCatalogController.class.getName());
    private static final String TAG = "Pump Accounting";
    private static final String ROW_LIMIT = "row-limit";
    private static final String ASCENDING = "ascending";
    private final MetricRegistry metrics;

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    public AccountingCatalogController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @NotNull
    protected WaterSupplyAccountingDao getWaterSupplyAccountingDao(DSLContext dsl) {
        return new WaterSupplyAccountingDao(dsl);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = START, description = "The start time of the time window for "
                + "pump accounting entries to retrieve. The format for this field is ISO 8601 extended, "
                + "with optional offset and timezone", required = true),
            @OpenApiParam(name = END, description = "The end time of the time window for pump "
                + "accounting entries to retrieve.", required = true),
            @OpenApiParam(name = TIMEZONE, description = "This field specifies a default timezone "
                + "to be used if the format of the " + END + " or " + BEGIN
                + " parameters do not include offset or time zone information. "
                + "Defaults to UTC."),
            @OpenApiParam(name = UNIT, description = "The unit of the flow rate of the accounting entries to "
                + "retrieve. Defaults to 'cms'."),
            @OpenApiParam(name = START_TIME_INCLUSIVE, description = "Whether or not the start time is "
                + "inclusive or not. Defaults to TRUE.", type = Boolean.class),
            @OpenApiParam(name = END_TIME_INCLUSIVE, description = "Whether or not the end time is inclusive "
                + "or not. Defaults to TRUE.", type = Boolean.class),
            @OpenApiParam(name = ASCENDING, description = "Whether or not the entries should be returned "
                + "in ascending order. Defaults to TRUE.", type = Boolean.class),
            @OpenApiParam(name = ROW_LIMIT, description = "The maximum number of rows to return. "
                + "Defaults to 0, which means no limit.", type = Integer.class)
        },
        pathParams = {
            @OpenApiParam(name = OFFICE, description = "The office ID of the project the "
                + "pump accounting is associated with.", required = true),
            @OpenApiParam(name = WATER_USER, description = "The water user the pump accounting is "
                + "associated with.", required = true),
            @OpenApiParam(name = CONTRACT_NAME, description = "The name of the contract associated with "
                + "the pump accounting.", required = true),
            @OpenApiParam(name = PROJECT_ID, description = "The project ID the pump accounting is "
                + "associated with.", required = true)
        },
        responses = {
            @OpenApiResponse(status = STATUS_200,
                content = {
                    @OpenApiContent(from = WaterSupplyAccounting.class, isArray = true,
                        type = Formats.JSONV1),
                    @OpenApiContent(from = WaterSupplyAccounting.class, isArray = true,
                        type = Formats.JSON)
                }),
            @OpenApiResponse(status = STATUS_404, description = "Pump Accounting not found for "
                + "provided input parameters."),
            @OpenApiResponse(status = STATUS_501, description = "Requested format is not implemented")
        },
        description = "Get pump accounting entries associated with a water supply contract.",
        path = "/projects/{office}/water-user/{water-user}/contracts/{contract-name}/accounting",
        method = HttpMethod.GET,
        tags = {TAG}
    )

    @Override
    public void handle(Context ctx) {
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            final String office = ctx.pathParam(OFFICE);
            final String waterUserName = ctx.pathParam(WATER_USER);
            final String contractId = ctx.pathParam(CONTRACT_NAME);
            final String locationId = ctx.pathParam(PROJECT_ID);
            final Instant startTime = requiredInstant(ctx, START);
            final Instant endTime = requiredInstant(ctx, END);
            final String units = ctx.queryParam(UNIT) != null ? ctx.queryParam(UNIT) : "cms";
            final boolean startInclusive = ctx.queryParam(START_TIME_INCLUSIVE) == null
                    || Boolean.parseBoolean(ctx.queryParam(START_TIME_INCLUSIVE));
            final boolean endInclusive = ctx.queryParam(END_TIME_INCLUSIVE) == null
                    || Boolean.parseBoolean(ctx.queryParam(END_TIME_INCLUSIVE));
            final boolean ascending = ctx.queryParam(ASCENDING) == null
                    || Boolean.parseBoolean(ctx.queryParam(ASCENDING));
            final int rowLimit = ctx.queryParam(ROW_LIMIT) != null ? Integer.parseInt(ctx.queryParam(ROW_LIMIT)) : 0;
            DSLContext dsl = getDslContext(ctx);

            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, WaterSupplyAccounting.class);
            ctx.contentType(contentType.toString());
            CwmsId projectLocation = new CwmsId.Builder().withOfficeId(office).withName(locationId).build();

            WaterContractDao contractDao = new WaterContractDao(dsl);
            WaterUser waterUser = contractDao.getWaterUser(projectLocation, waterUserName);
            List<WaterUserContract> contract = contractDao.getAllWaterContracts(projectLocation,
                    waterUser.getEntityName());

            if (waterUser.getEntityName() == null) {
                CdaError error = new CdaError("Unable to retrieve accounting - no water user found for the"
                        + " provided parameters.");
                LOGGER.log(Level.SEVERE, "Error retrieving water pump accounting - no water user found.");
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(error);
                return;
            }

            boolean contractExists = false;
            for (WaterUserContract contractItem : contract) {
                if (contractItem.getContractId().getName().equals(contractId)) {
                    contractExists = true;
                    break;
                }
            }

            if (!contractExists) {
                CdaError error = new CdaError("Unable to retrieve accounting - no matching contract found for the"
                        + " provided parameters.");
                LOGGER.log(Level.SEVERE, "Error retrieving water pump accounting - no contract found.");
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(error);
                return;
            }

            WaterSupplyAccountingDao waterSupplyAccountingDao = getWaterSupplyAccountingDao(dsl);
            List<WaterSupplyAccounting> accounting = waterSupplyAccountingDao.retrieveAccounting(contractId, waterUser,
                    projectLocation, units, startTime, endTime, startInclusive, endInclusive,
                    ascending, rowLimit);

            String result = Formats.format(contentType, accounting, WaterSupplyAccounting.class);
            ctx.result(result);
            ctx.status(HttpServletResponse.SC_OK);
        }
    }
}
