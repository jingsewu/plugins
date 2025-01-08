package com.swms.plugins.print.config;

import lombok.Data;

import java.util.Map;

@Data
public class PrintPluginConfig {

    private String authorization;
    private String firstLabelUrl;
    private String splitUrl;
    private String addToLabelUrl;
    private String addToSplitUrl;

    private Map<String, PrintConfig> stationPrintConfig;

    private PrintConfig manualAreaPrintConfig;
}
