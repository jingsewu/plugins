package com.swms.plugins.print.config;

import com.swms.wms.api.printer.constants.LabelTypeEnum;
import lombok.Data;

import java.util.Map;

@Data
public class PrintPluginConfig {

    private String authorization;
    private String firstLabelUrl;
    private String splitUrl;
    private String addToLabelUrl;
    private String addToSplitUrl;

    private Map<String, Map<LabelTypeEnum, PrintConfig>> stationPrintConfig;
}
