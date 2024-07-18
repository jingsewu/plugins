package com.swms.plugins.print;

import com.swms.common.utils.user.UserContext;
import com.swms.common.utils.utils.JsonUtils;
import com.swms.plugin.extend.extensions.OperationContext;
import com.swms.plugin.extend.extensions.configuration.TenantPluginConfig;
import com.swms.plugin.extend.wms.outbound.PrintPlugin;
import com.swms.plugins.print.config.PrintConfig;
import com.swms.plugins.print.config.PrintPluginConfig;
import com.swms.plugins.print.dto.PrintRequestDTO;
import com.swms.user.api.UserApi;
import com.swms.user.api.dto.UserDTO;
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
import java.util.Objects;

@Slf4j
@Extension
@RequiredArgsConstructor
public class SentrixMobileLabelPrintPlugin implements PrintPlugin {

    private static final String PLUGIN_ID = "Sentrix-Mobile-Label-Print-Plugin-0.0.1";
    private static final String PRINT_URL = "http://$host:$port/print";

    private final IPutWallApi putWallApi;
    private final IPickingOrderApi pickingOrderApi;
    private final UserApi userApi;
    private final IOutboundPlanOrderApi outboundPlanOrderApi;
    private final ITransferContainerApi transferContainerApi;

    @Override
    public Void doOperation(OperationContext<PrintEvent> operationContext) {
        PrintEvent event = operationContext.getOperationObject();
        log.info("received print event: {}", event);

        Long workStationId = event.getWorkStationId();
        if (workStationId != null) {
            Object parameter = event.getParameter();
            PutWallSlotDTO putWallSlot = putWallApi.getPutWallSlot((String) parameter, workStationId);
            if (putWallSlot == null) {
                log.warn("cannot find putWallSlot use work station id : {}, parameter : {}", workStationId, parameter);
                return null;
            }
            if (PutWallSlotStatusEnum.BOUND.equals(putWallSlot.getPutWallSlotStatus()) || putWallSlot.getPickingOrderId() == null) {
                log.warn("put wall slot is not bounding, or picking order id is null, work station id: {}, put wall slot: {}, put wall slot status: {}",
                    workStationId, putWallSlot.getPutWallSlotCode(), putWallSlot.getPutWallSlotStatus());
                return null;
            }
            PickingOrderDTO pickingOrderDTO = pickingOrderApi.getById(putWallSlot.getPickingOrderId());
            if (pickingOrderDTO == null || PickingOrderStatusEnum.isFinalStatues(pickingOrderDTO.getPickingOrderStatus())) {
                log.warn("picking order is null, or picking order is finished, picking order id: {}", putWallSlot.getPickingOrderId());
                return null;
            }

            PrintConfig printConfig = getWorkStationPrintConfig(workStationId);
            triggerPrint(printConfig, pickingOrderDTO);
        } else if (StringUtils.isNotEmpty(UserContext.getCurrentUser())) {
            String userName = UserContext.getCurrentUser();
            UserDTO user = userApi.getByUsername(userName);

            List<PickingOrderDTO> pickingOrderDTOS = pickingOrderApi.findUncompletedByReceivedUserId(user.getId());

            if (CollectionUtils.isEmpty(pickingOrderDTOS)) {
                log.warn("connot find any received picking order");
            }
            PrintConfig printConfig = getManualAreaPrintConfig((String) event.getParameter());

            pickingOrderDTOS.stream().findAny().ifPresent(pickingOrderDTO -> triggerPrint(printConfig, pickingOrderDTO));
        } else {
            log.error("cannot find work station info or current user info, so we cannot trigger print");
        }

        return null;
    }

    private PrintConfig getWorkStationPrintConfig(Long workStationId) {
        PrintPluginConfig tenantConfig = TenantPluginConfig.getTenantConfig(PLUGIN_ID, PrintPluginConfig.class);
        return tenantConfig.getStationPrintConfig().get(String.valueOf(workStationId));
    }

    private PrintConfig getManualAreaPrintConfig(String locationCode) {
        PrintPluginConfig tenantConfig = TenantPluginConfig.getTenantConfig(PLUGIN_ID, PrintPluginConfig.class);
        return tenantConfig.getManualAreaPrintConfig().get(locationCode);
    }

    private void triggerPrint(PrintConfig config, PickingOrderDTO pickingOrderDTO) {
        String printURL = replacePrintURL(config);
        PrintRequestDTO requestDTO = buildPrintRequestDTO(config.getPrintName(), pickingOrderDTO);
        if (requestDTO == null) {
            log.error("build print request failed, picking order dto: {}", pickingOrderDTO);
            return;
        }

        RestTemplate template = new RestTemplate();
        HttpEntity<String> entity = new HttpEntity<>(JsonUtils.obj2String(requestDTO));
        template.postForLocation(printURL, entity);
    }

    private String replacePrintURL(PrintConfig config) {
        return PRINT_URL.replace("$host", config.getHost()).replace("$port", String.valueOf(config.getPort()));
    }

    private PrintRequestDTO buildPrintRequestDTO(String printName, PickingOrderDTO pickingOrderDTO) {
        String pdfUrl = findOrderPdfUrl(pickingOrderDTO);
        if (StringUtils.isEmpty(pdfUrl)) {
            return null;
        }

        PrintRequestDTO.Printer print = PrintRequestDTO.Printer.builder().name(printName).build();
        PrintRequestDTO.Options options = PrintRequestDTO.Options.builder().build();

        PrintRequestDTO.Data data = PrintRequestDTO.Data.builder().data(pdfUrl).build();

        return PrintRequestDTO.builder().printer(print).options(options).data(List.of(data)).build();
    }

    private String findOrderPdfUrl(PickingOrderDTO pickingOrderDTO) {
        List<OutboundPlanOrderDTO> outboundPlanOrderDTOS = outboundPlanOrderApi.findByWaveNos(List.of(pickingOrderDTO.getWaveNo()), false);
        if (CollectionUtils.isEmpty(outboundPlanOrderDTOS)) {
            return null;
        }
        if (outboundPlanOrderDTOS.stream().anyMatch(v -> OutboundOrderInnerTypeEnum.REPLENISH_OUTBOUND_ORDER.name().equals(v.getCustomerOrderType()))) {
            log.info("This is refill order, don't need to print any label");
            return null;
        }

        boolean isParentWave = outboundPlanOrderDTOS.stream().anyMatch(v -> v.getCustomerOrderNo().equals(v.getCustomerWaveNo()));

        List<TransferContainerRecordDTO> transferContainerRecordDTOS = transferContainerApi.findByPickingOrderIds(List.of(pickingOrderDTO.getId()));
        boolean isSplitFinished = transferContainerRecordDTOS.stream().anyMatch(v -> v.getTransferContainerStatus() == TransferContainerRecordStatusEnum.SEALED);

        PrintPluginConfig printPluginConfig = getPrintPluginConfig();
        BooleanPair pair = BooleanPair.valueOf(isParentWave, isSplitFinished);
        String requestURL = getRequestURL(pair, outboundPlanOrderDTOS.stream().findAny().get().getCustomerWaveNo(), printPluginConfig);
        if (StringUtils.isEmpty(requestURL)) {
            log.warn("cannot get pdf url, picking order info: {}", pickingOrderDTO);
            return null;
        }

        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", printPluginConfig.getAuthorization());
        headers.add("Content-Type", "application/json");

        HttpEntity<String> entity = new HttpEntity<>("", headers);

        RestTemplate template = new RestTemplate();
        ResponseEntity<PdfUrlResponse> responseEntity = template.exchange(requestURL, HttpMethod.GET, entity, PdfUrlResponse.class);
        if (!PdfUrlResponse.STATUS_SUCCESS.equals(Objects.requireNonNull(responseEntity.getBody()).getStatus())) {
            log.warn("MA response error, picking order id: {}, response: {}", pickingOrderDTO.getId(), responseEntity);
            return null;
        }

        return responseEntity.getBody().getUrl();
    }

    private PrintPluginConfig getPrintPluginConfig() {
        return TenantPluginConfig.getTenantConfig(PLUGIN_ID, PrintPluginConfig.class);
    }

    private String getRequestURL(BooleanPair pair, String customerWaveNo, PrintPluginConfig printPluginConfig) {
        String requestURL = null;
        switch (pair) {
            case TRUE_FALSE -> requestURL = printPluginConfig.getFirstLabelUrl();
            case TRUE_TRUE -> requestURL = printPluginConfig.getSplitUrl();
            case FALSE_TRUE -> requestURL = printPluginConfig.getAddToLabelUrl();
            case FALSE_FALSE -> requestURL = printPluginConfig.getAddToSplitUrl();
        }
        return Objects.requireNonNull(requestURL).replace("$customerWaveNo", customerWaveNo);
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

        /**
         * Static factory method to get the BooleanPair instance based on two boolean values.
         *
         * @param first  the first boolean value
         * @param second the second boolean value
         * @return the corresponding BooleanPair instance
         */
        public static BooleanPair valueOf(boolean first, boolean second) {
            return first && second ? TRUE_TRUE :
                first ? TRUE_FALSE :
                    second ? FALSE_TRUE :
                        FALSE_FALSE;
        }
    }

    @Data
    public static class PdfUrlResponse {
        public static final String STATUS_SUCCESS = "1";
        private String status;
        private String url;
    }
}
