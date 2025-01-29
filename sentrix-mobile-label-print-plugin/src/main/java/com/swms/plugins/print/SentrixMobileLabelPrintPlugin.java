package com.swms.plugins.print;

import com.swms.common.utils.utils.JsonUtils;
import com.swms.plugin.extend.extensions.OperationContext;
import com.swms.plugin.extend.extensions.configuration.TenantPluginConfig;
import com.swms.plugin.extend.wms.outbound.PrintPlugin;
import com.swms.plugins.print.config.PrintConfig;
import com.swms.plugins.print.config.PrintPluginConfig;
import com.swms.plugins.print.dto.PrintRequestDTO;
import com.swms.wms.api.basic.IPutWallApi;
import com.swms.wms.api.basic.constants.PutWallSlotStatusEnum;
import com.swms.wms.api.basic.dto.PutWallSlotDTO;
import com.swms.wms.api.outbound.IOutboundPlanOrderApi;
import com.swms.wms.api.outbound.IPickingOrderApi;
import com.swms.wms.api.outbound.constants.OutboundOrderInnerTypeEnum;
import com.swms.wms.api.outbound.constants.PickingOrderStatusEnum;
import com.swms.wms.api.outbound.dto.OutboundCustomLabelDTO;
import com.swms.wms.api.outbound.dto.OutboundPlanOrderDTO;
import com.swms.wms.api.outbound.dto.PickingOrderDTO;
import com.swms.wms.api.printer.constants.LabelTypeEnum;
import com.swms.wms.api.printer.constants.PrintNodeEnum;
import com.swms.wms.api.printer.event.PrintEvent;
import com.swms.wms.api.task.ITransferContainerApi;
import com.swms.wms.api.task.constants.TransferContainerRecordStatusEnum;
import com.swms.wms.api.task.dto.TransferContainerRecordDTO;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.pf4j.Extension;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Extension
@RequiredArgsConstructor
public class SentrixMobileLabelPrintPlugin implements PrintPlugin {

    private static final String OUTBOUND_WAVE_NO_PREFIX = "WAVE_";

    private static final String PLUGIN_ID = "Sentrix-Mobile-Label-Print-Plugin-0.0.1";
    private static final String PRINT_URL = "http://$host:$port/print";

    private final IPutWallApi putWallApi;
    private final IPickingOrderApi pickingOrderApi;
    private final IOutboundPlanOrderApi outboundPlanOrderApi;
    private final ITransferContainerApi transferContainerApi;

    @Override
    public Void doOperation(OperationContext<PrintEvent> operationContext) {
        PrintEvent event = operationContext.getOperationObject();
        log.info("Received print event: {}", event);

        Long workStationId = event.getWorkStationId();
        if (workStationId == null) {
            log.warn("Work station id is null， event: {}", event);
            return null;
        }

        handleWorkStationPrint(workStationId, event);
        return null;
    }

    /**
     * Handle print logic when a workstation ID is available.
     */
    private void handleWorkStationPrint(Long workStationId, PrintEvent event) {

        if (LabelTypeEnum.SKU == event.getLabelType()) {
            // Retrieve config and trigger print
            PrintConfig printConfig = getWorkStationPrintConfig(workStationId, LabelTypeEnum.SKU);
            if (printConfig == null) {
                return;
            }

            List<OutboundCustomLabelDTO> customLabelDTOS = (List<OutboundCustomLabelDTO>) event.getParameter();
            triggerSkuLabelPrint(printConfig, customLabelDTOS);
        } else {
            String parameter = String.valueOf(event.getParameter());
            String waveNo = transferToWaveNo(parameter, workStationId);

            if (StringUtils.isEmpty(waveNo)) {
                log.warn("Cannot find wave no, event id: {}", event.getEventId());
                return;
            }

            // Retrieve config and trigger print
            triggerPrintLabelByWaveNo(waveNo, event);
        }
    }

    private void triggerSkuLabelPrint(PrintConfig config, List<OutboundCustomLabelDTO> customLabelDTOS) {
        if (CollectionUtils.isEmpty(customLabelDTOS)) {
            log.warn("CustomLabels is empty");
            return;
        }

        customLabelDTOS.forEach(labelDTO -> triggerPrint(config, labelDTO.getUrl()));
    }

    private String transferToWaveNo(String parameter, Long workStationId) {
        if (StringUtils.startsWith(parameter, OUTBOUND_WAVE_NO_PREFIX)) {
            return parameter;
        }

        PutWallSlotDTO putWallSlot = putWallApi.getPutWallSlot(parameter, workStationId);
        // Check slot status and picking order
        if (PutWallSlotStatusEnum.BOUND.equals(putWallSlot.getPutWallSlotStatus())
                || putWallSlot.getPickingOrderId() == null) {

            log.warn("PutWallSlot not bound or picking order is null, station: {}, slot code: {}, status: {}",
                    workStationId, putWallSlot.getPutWallSlotCode(), putWallSlot.getPutWallSlotStatus());
            return null;
        }

        PickingOrderDTO pickingOrderDTO = pickingOrderApi.getById(putWallSlot.getPickingOrderId());
        if (pickingOrderDTO == null
                || PickingOrderStatusEnum.isFinalStatues(pickingOrderDTO.getPickingOrderStatus())) {

            log.warn("Picking order is null or already finished, order ID: {}", putWallSlot.getPickingOrderId());
            return null;
        }

        return pickingOrderDTO.getWaveNo();
    }

    /**
     * Fetch printer config for a given workstation ID.
     */
    private PrintConfig getWorkStationPrintConfig(Long workStationId, LabelTypeEnum labelType) {
        PrintPluginConfig tenantConfig = TenantPluginConfig.getTenantConfig(PLUGIN_ID, PrintPluginConfig.class);
        Map<LabelTypeEnum, PrintConfig> labelTypePrintConfigMap = tenantConfig.getStationPrintConfig().get(String.valueOf(workStationId));
        if (labelTypePrintConfigMap == null || labelTypePrintConfigMap.isEmpty()) {
            log.warn("Cannot find print config for work station: {}", workStationId);
            return null;
        }
        PrintConfig printConfig = labelTypePrintConfigMap.get(labelType);
        if (printConfig == null) {
            log.warn("Cannot find print config for station {} label type: {}", workStationId, labelType);
            return null;
        }
        return printConfig;
    }

    /**
     * Build a print request DTO and call the external print service.
     */
    private void triggerPrintLabelByWaveNo(String waveNo, PrintEvent event) {
        if (PrintNodeEnum.PRINT_NODE_CLICK_REPRINT == event.getPrintNode()) {
            reprint(waveNo, event);
            return;
        }

        // 打印快递 label
        String labelPdfUrl = findOrderPdfUrl(waveNo);
        if (StringUtils.isNotEmpty(labelPdfUrl)) {
            PrintConfig printConfig = getWorkStationPrintConfig(event.getWorkStationId(), LabelTypeEnum.LABEL);
            if (printConfig == null) {
                return;
            }
            triggerPrint(printConfig, labelPdfUrl);
        }

        // 打印 a4 paper
        String a4PaperUrl = findA4PaperUrl(waveNo);
        if (StringUtils.isNotEmpty(a4PaperUrl)) {
            PrintConfig printConfig = getWorkStationPrintConfig(event.getWorkStationId(), LabelTypeEnum.A4PAPER);
            if (printConfig == null) {
                return;
            }
            triggerPrint(printConfig, a4PaperUrl);
        }
        log.info("Print label and a4paper for Wave NO: {}", waveNo);
    }

    private void reprint(String waveNo, PrintEvent event) {
        // Retrieve config and trigger print
        PrintConfig printConfig = getWorkStationPrintConfig(event.getWorkStationId(), event.getLabelType());
        if (printConfig == null) {
            return;
        }
        String pdfUrl;
        if (LabelTypeEnum.LABEL == event.getLabelType()) {
            pdfUrl = findOrderPdfUrl(waveNo);
        } else {
            pdfUrl = findA4PaperUrl(waveNo);
        }

        if (StringUtils.isEmpty(pdfUrl)) {
            log.warn("Cannot find pdfUrl for Wave NO: {} for printer: {}", waveNo, printConfig.getPrintName());
            return;
        }
        triggerPrint(printConfig, pdfUrl);

        log.info("Reprint label for Wave NO: {}, label type: {}", waveNo, event.getLabelType());
    }

    private String buildPrintUrl(PrintConfig config) {
        return PRINT_URL
                .replace("$host", config.getHost())
                .replace("$port", String.valueOf(config.getPort()));
    }

    private void triggerPrint(PrintConfig config, String pdfUrl) {
        String printURL = buildPrintUrl(config);

        PrintRequestDTO requestDTO = buildPrintRequestDTO(config.getPrintName(), pdfUrl);
        if (requestDTO == null) {
            log.error("Failed to build PrintRequestDTO, skipping print. pdf url: {}", pdfUrl);
            return;
        }

        RestTemplate template = new RestTemplate();
        HttpEntity<String> entity = new HttpEntity<>(JsonUtils.obj2String(requestDTO));
        template.postForLocation(printURL, entity);
    }

    /**
     * Construct the print request DTO with printer name and PDF data.
     */
    private PrintRequestDTO buildPrintRequestDTO(String printName, String pdfUrl) {
        PrintRequestDTO.Printer print = PrintRequestDTO.Printer.builder().name(printName).build();
        PrintRequestDTO.Options options = PrintRequestDTO.Options.builder().build();

        PrintRequestDTO.Data data = PrintRequestDTO.Data.builder().data(pdfUrl).build();

        return PrintRequestDTO.builder().printer(print).options(options).data(List.of(data)).build();
    }

    /**
     * Look up the PDF URL for the given picking order.
     */
    private String findOrderPdfUrl(String waveNo) {
        List<OutboundPlanOrderDTO> outboundPlanOrders = outboundPlanOrderApi.findByWaveNos(
                List.of(waveNo), false);

        // If no plan orders found or it is a replenish order, return null
        if (CollectionUtils.isEmpty(outboundPlanOrders)
                || outboundPlanOrders.stream().anyMatch(o ->
                OutboundOrderInnerTypeEnum.REPLENISH_OUTBOUND_ORDER.name().equals(o.getCustomerOrderType()))) {

            log.info("No valid outbound plan orders or is a refill order, skipping label print.");
            return null;
        }

        boolean isParentWave = outboundPlanOrders.stream()
                .anyMatch(v -> v.getCustomerOrderNo().equals(v.getCustomerWaveNo()));

        List<Long> pickingOrderIds = pickingOrderApi.findPickingOrderByWaveNo(waveNo).stream()
                .map(PickingOrderDTO::getId).toList();
        // Retrieve container records for the current picking order
        List<TransferContainerRecordDTO> containerRecords = transferContainerApi.findByPickingOrderIds(pickingOrderIds);

        boolean isSplitFinished = containerRecords.stream()
                .anyMatch(v -> v.getTransferContainerStatus()
                        == TransferContainerRecordStatusEnum.SEALED);

        String customerWaveNo = outboundPlanOrders.get(0).getCustomerWaveNo();

        // Generate the request URL for fetching PDF
        PrintPluginConfig config = getPrintPluginConfig();
        BooleanPair pair = BooleanPair.valueOf(isParentWave, isSplitFinished);
        String requestUrl = resolveRequestUrl(pair, customerWaveNo, config);
        if (StringUtils.isEmpty(requestUrl)) {
            log.warn("Cannot resolve PDF URL; Wave NO: {}", waveNo);
            return null;
        }

        log.debug("try get pdf url for wave no: {}, customer wave no: {}", waveNo, customerWaveNo);

        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", config.getAuthorization());
        headers.add("Content-Type", "application/json");

        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<PdfUrlResponse> response = restTemplate.exchange(
                requestUrl,
                HttpMethod.GET,
                new HttpEntity<>("", headers),
                PdfUrlResponse.class
        );

        PdfUrlResponse body = response.getBody();
        if (body == null || !PdfUrlResponse.STATUS_SUCCESS.equals(body.getStatus())) {
            log.warn("MA service returned invalid response, Wave NO: {}, response: {}", waveNo, response);
            return null;
        }

        log.debug("successful get label pdf url, wave no : {}, customer wave no : {} : {}", waveNo, customerWaveNo, body.getUrl());

        return body.getUrl();
    }

    private String findA4PaperUrl(String waveNo) {
        boolean hasA4PdfUrl = transferContainerApi.assertWaveHasA4PdfUrl(waveNo);
        if (!hasA4PdfUrl) {
            log.info("No A4Paper found for Wave NO: {}", waveNo);
            return null;
        }

        List<OutboundPlanOrderDTO> outboundPlanOrderDTOS = outboundPlanOrderApi.findByWaveNos(List.of(waveNo), false);
        if (CollectionUtils.isEmpty(outboundPlanOrderDTOS)) {
            log.warn("Cannot find outbound order for wave no: {}", waveNo);
            return null;
        }

        return outboundPlanOrderDTOS.stream()
                .map(OutboundPlanOrderDTO::getA4Paper)
                .filter(StringUtils::isNotEmpty)
                .findFirst().orElse(null);
    }

    private PrintPluginConfig getPrintPluginConfig() {
        return TenantPluginConfig.getTenantConfig(PLUGIN_ID, PrintPluginConfig.class);
    }

    private String resolveRequestUrl(BooleanPair pair, String customerWaveNo, PrintPluginConfig config) {
        String baseUrl;
        switch (pair) {
            case TRUE_FALSE -> baseUrl = config.getFirstLabelUrl();
            case TRUE_TRUE -> baseUrl = config.getSplitUrl();
            case FALSE_TRUE -> baseUrl = config.getAddToSplitUrl();
            case FALSE_FALSE -> baseUrl = config.getAddToLabelUrl();
            default -> {
                log.warn("Unexpected BooleanPair: {}", pair);
                return null;
            }
        }
        return Objects.requireNonNull(baseUrl).replace("$customerWaveNo", customerWaveNo);
    }

    @AllArgsConstructor
    private enum BooleanPair {
        // 首波次，包含父单拆箱后的
        TRUE_TRUE(true, true),
        // 首波次，包含父单
        TRUE_FALSE(true, false),
        // 次波次拆箱后的
        FALSE_TRUE(false, true),
        // 次波次
        FALSE_FALSE(false, false);

        private boolean isParentWave;
        private boolean isSplitFinished;

        public static BooleanPair valueOf(boolean first, boolean second) {
            if (first && second) {
                return TRUE_TRUE;
            }
            if (first) {
                return TRUE_FALSE;
            }
            if (second) {
                return FALSE_TRUE;
            }
            return FALSE_FALSE;
        }
    }

    @Data
    public static class PdfUrlResponse {
        public static final String STATUS_SUCCESS = "1";
        private String status;
        private String url;
    }
}
