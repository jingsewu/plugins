package com.swms.plugins.print.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class PrintRequestDTO {

    private Printer printer;
    private Options options;
    private List<Data> data;

    @lombok.Data
    @Builder
    public static class Printer {
        @Builder.Default private String name = "ZDesigner";
    }

    @lombok.Data
    @Builder
    public static class Options {
        private String bounds;
        @Builder.Default
        private String colorType = "color";
        @Builder.Default
        private Integer copies = 1;
        @Builder.Default
        private Integer density = 0;
        @Builder.Default
        private Boolean duplex = false;
        private String fallbackDensity;
        @Builder.Default
        private String interpolation = "bicubic";
        private String jobName;
        @Builder.Default
        private Boolean legacy = false;
        @Builder.Default
        private Integer margins = 0;
        private String orientation;
        private String paperThickness;
        private String printerTray;
        @Builder.Default
        private Boolean rasterize = false;
        @Builder.Default
        private Integer rotation = 0;
        @Builder.Default
        private Boolean scaleContent = true;
        private Integer size;
        @Builder.Default
        private String units = "in";
        @Builder.Default
        private Boolean forceRaw = false;
        private String encoding;
        private String spool;
    }

    @lombok.Data
    @Builder
    public static class Data {
        @Builder.Default private String type = "pixel";
        @Builder.Default private String format = "pdf";
        @Builder.Default private String flavor = "file";
        private String data;
    }
}
