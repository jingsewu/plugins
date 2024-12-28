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
import com.swms.wms.api.outbound.dto.OutboundPlanOrderDTO;
import com.swms.wms.api.outbound.dto.PickingOrderDTO;
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
import java.util.Optional;

@Slf4j
@Extension
@RequiredArgsConstructor
public class SentrixMobileLabelPrintPlugin implements PrintPlugin {

    private static final String PLUGIN_ID = "Sentrix-Mobile-Label-Print-Plugin-0.0.1";
    private static final String PRINT_URL = "http://$host:$port/print";

    private static final String PICKING_ORDER_ID = "pickingOrderId";

    private final IPutWallApi putWallApi;
    private final IPickingOrderApi pickingOrderApi;
    private final IOutboundPlanOrderApi outboundPlanOrderApi;
    private final ITransferContainerApi transferContainerApi;

    @Override
    public Void doOperation(OperationContext<PrintEvent> operationContext) {
        PrintEvent event = operationContext.getOperationObject();
        log.info("Received print event: {}", event);

        Long workStationId = event.getWorkStationId();
        if (workStationId != null) {
            handleWorkStationPrint(workStationId, event);
        } else {
            handleManualAreaPrint(event);
        }
        return null;
    }

    /**
     * Handle print logic when a workstation ID is available.
     */
    private void handleWorkStationPrint(Long workStationId, PrintEvent event) {
        String parameter = String.valueOf(event.getParameter());
        PutWallSlotDTO putWallSlot = putWallApi.getPutWallSlot(parameter, workStationId);

        if (putWallSlot == null) {
            log.warn("Cannot find PutWallSlot for workstation ID: {}, parameter: {}", workStationId, parameter);
            return;
        }

        // Check slot status and picking order
        if (PutWallSlotStatusEnum.BOUND.equals(putWallSlot.getPutWallSlotStatus())
                || putWallSlot.getPickingOrderId() == null) {

            log.warn("PutWallSlot not bound or picking order is null, station: {}, slot code: {}, status: {}",
                    workStationId, putWallSlot.getPutWallSlotCode(), putWallSlot.getPutWallSlotStatus());
            return;
        }

        PickingOrderDTO pickingOrderDTO = pickingOrderApi.getById(putWallSlot.getPickingOrderId());
        if (pickingOrderDTO == null
                || PickingOrderStatusEnum.isFinalStatues(pickingOrderDTO.getPickingOrderStatus())) {

            log.warn("Picking order is null or already finished, order ID: {}", putWallSlot.getPickingOrderId());
            return;
        }

        // Retrieve config and trigger print
        PrintConfig printConfig = getWorkStationPrintConfig(workStationId);
        triggerPrint(printConfig, pickingOrderDTO);
    }

    /**
     * Handle print logic for manual area (no workstation ID).
     */
    private void handleManualAreaPrint(PrintEvent event) {
        // Attempt to fetch pickingOrderId from target arguments
        Optional<Map<String, Object>> args = event.getTargetArgs().stream().findAny();
        if (args.isEmpty() || !args.get().containsKey(PICKING_ORDER_ID)) {
            log.warn("Cannot find pickingOrderId parameter in targetArgs.");
            return;
        }

        Long pickingOrderId = Long.parseLong(args.get().get(PICKING_ORDER_ID).toString());
        PickingOrderDTO pickingOrderDTO = pickingOrderApi.getById(pickingOrderId);
        if (pickingOrderDTO == null) {
            log.warn("Cannot find picking order by ID: {}", pickingOrderId);
            return;
        }

        // Retrieve config for manual area
        String locationCode = String.valueOf(event.getParameter());
        PrintConfig printConfig = getManualAreaPrintConfig(locationCode);
        triggerPrint(printConfig, pickingOrderDTO);
    }

    /**
     * Fetch printer config for a given workstation ID.
     */
    private PrintConfig getWorkStationPrintConfig(Long workStationId) {
        PrintPluginConfig tenantConfig = TenantPluginConfig.getTenantConfig(PLUGIN_ID, PrintPluginConfig.class);
        return tenantConfig.getStationPrintConfig().get(String.valueOf(workStationId));
    }

    /**
     * Fetch printer config for a manual location code.
     */
    private PrintConfig getManualAreaPrintConfig(String locationCode) {
        PrintPluginConfig tenantConfig = TenantPluginConfig.getTenantConfig(PLUGIN_ID, PrintPluginConfig.class);
        return tenantConfig.getManualAreaPrintConfig().get(locationCode);
    }

    /**
     * Build a print request DTO and call the external print service.
     */
    private void triggerPrint(PrintConfig config, PickingOrderDTO pickingOrderDTO) {
        String printURL = buildPrintUrl(config);
        PrintRequestDTO requestDTO = buildPrintRequestDTO(config.getPrintName(), pickingOrderDTO);
        if (requestDTO == null) {
            log.error("Failed to build PrintRequestDTO, skipping print. Order: {}", pickingOrderDTO);
            return;
        }

        RestTemplate template = new RestTemplate();
        HttpEntity<String> entity = new HttpEntity<>(JsonUtils.obj2String(requestDTO));
        template.postForLocation(printURL, entity);
    }

    private String buildPrintUrl(PrintConfig config) {
        return PRINT_URL
                .replace("$host", config.getHost())
                .replace("$port", String.valueOf(config.getPort()));
    }

    /**
     * Construct the print request DTO with printer name and PDF data.
     */
    private PrintRequestDTO buildPrintRequestDTO(String printName, PickingOrderDTO pickingOrderDTO) {
        String pdfUrl = findOrderPdfUrl(pickingOrderDTO);
        if (StringUtils.isEmpty(pdfUrl)) {
            log.warn("Cannot find pdfUrl for pickingOrder: {} on print: {}", pickingOrderDTO.getId(), printName);
            return null;
        }

        PrintRequestDTO.Printer print = PrintRequestDTO.Printer.builder().name(printName).build();
        PrintRequestDTO.Options options = PrintRequestDTO.Options.builder().build();

        PrintRequestDTO.Data data = PrintRequestDTO.Data.builder().data(pdfUrl).build();

        return PrintRequestDTO.builder().printer(print).options(options).data(List.of(data)).build();
    }

    /**
     * Look up the PDF URL for the given picking order.
     */
    private String findOrderPdfUrl(PickingOrderDTO pickingOrderDTO) {
        List<OutboundPlanOrderDTO> outboundPlanOrders = outboundPlanOrderApi.findByWaveNos(
                List.of(pickingOrderDTO.getWaveNo()), false);

        // If no plan orders found or it is a replenish order, return null
        if (CollectionUtils.isEmpty(outboundPlanOrders)
                || outboundPlanOrders.stream().anyMatch(o ->
                OutboundOrderInnerTypeEnum.REPLENISH_OUTBOUND_ORDER.name().equals(o.getCustomerOrderType()))) {

            log.info("No valid outbound plan orders or is a refill order, skipping label print.");
            return null;
        }

        boolean isParentWave = outboundPlanOrders.stream()
                .anyMatch(v -> v.getCustomerOrderNo().equals(v.getCustomerWaveNo()));

        // Retrieve container records for the current picking order
        List<TransferContainerRecordDTO> containerRecords = transferContainerApi
                .findByPickingOrderIds(List.of(pickingOrderDTO.getId()));

        boolean isSplitFinished = containerRecords.stream()
                .anyMatch(v -> v.getTransferContainerStatus()
                        == TransferContainerRecordStatusEnum.SEALED);

        // Generate the request URL for fetching PDF
        PrintPluginConfig config = getPrintPluginConfig();
        BooleanPair pair = BooleanPair.valueOf(isParentWave, isSplitFinished);
        String requestUrl = resolveRequestUrl(pair, outboundPlanOrders.get(0).getCustomerWaveNo(), config);
        if (StringUtils.isEmpty(requestUrl)) {
            log.warn("Cannot resolve PDF URL; picking order info: {}", pickingOrderDTO);
            return null;
        }

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
            log.warn("MA service returned invalid response, order ID: {}, response: {}", pickingOrderDTO.getId(), response);
            return null;
        }

        return body.getUrl();
    }

    private PrintPluginConfig getPrintPluginConfig() {
        return TenantPluginConfig.getTenantConfig(PLUGIN_ID, PrintPluginConfig.class);
    }

    private String resolveRequestUrl(BooleanPair pair, String customerWaveNo, PrintPluginConfig config) {
        String baseUrl;
        switch (pair) {
            case TRUE_FALSE -> baseUrl = config.getFirstLabelUrl();
            case TRUE_TRUE -> baseUrl = config.getSplitUrl();
            case FALSE_TRUE -> baseUrl = config.getAddToLabelUrl();
            case FALSE_FALSE -> baseUrl = config.getAddToSplitUrl();
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
