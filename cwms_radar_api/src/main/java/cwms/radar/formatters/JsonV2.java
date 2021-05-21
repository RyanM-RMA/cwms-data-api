package cwms.radar.formatters;

import java.util.List;

import cwms.radar.data.dto.Catalog;
import cwms.radar.data.dto.CwmsDTO;
import cwms.radar.data.dto.Location;
import cwms.radar.data.dto.Office;
import io.javalin.plugin.json.JavalinJson;
import service.annotations.FormatService;

@FormatService(contentType = Formats.JSONV2, dataTypes = {Office.class,Location.class,Catalog.class})
public class JsonV2 implements OutputFormatter {

	@Override
	public String getContentType() {		
		return Formats.JSONV2;
	}

	@Override
	public String format(CwmsDTO dto) {		
		return JavalinJson.toJson(dto);
	}

	@Override
	public String format(List<? extends CwmsDTO> dtoList) {		
		return JavalinJson.toJson(dtoList);
	}

}
