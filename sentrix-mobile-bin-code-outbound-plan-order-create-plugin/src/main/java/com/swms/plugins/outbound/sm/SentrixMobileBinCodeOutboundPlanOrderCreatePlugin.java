package com.swms.plugins.outbound.sm;

import com.swms.common.utils.utils.DateFormatUtil;
import com.swms.domain.event.DomainEventPublisher;
import com.swms.mdm.api.config.constants.ParserObjectEnum;
import com.swms.mdm.api.main.data.ISkuMainDataApi;
import com.swms.mdm.api.main.data.dto.BarcodeDTO;
import com.swms.mdm.api.main.data.dto.SkuMainDataDTO;
import com.swms.plugin.extend.extensions.OperationContext;
import com.swms.plugin.extend.wms.outbound.IOutboundPlanOrderCreatePlugin;
import com.swms.wms.api.basic.IContainerApi;
import com.swms.wms.api.basic.dto.ContainerDTO;
import com.swms.wms.api.outbound.dto.OutboundPlanOrderDTO;
import com.swms.wms.api.stock.ISkuBatchAttributeApi;
import com.swms.wms.api.stock.dto.SkuBatchAttributeDTO;
import com.swms.wms.api.stock.dto.StockCreateDTO;
import com.swms.wms.api.stock.event.StockCreateEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.pf4j.Extension;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Extension
@RequiredArgsConstructor
public class SentrixMobileBinCodeOutboundPlanOrderCreatePlugin implements IOutboundPlanOrderCreatePlugin {

    // 人工区 8 区格口位置字段
    private static final String BIN_CODE = "binCode";
    private static final String BAR_CODE = "barcode";

    private static final String TARGET_CONTAINER_FACE = "F";
    private static final String TARGET_CONTAINER_SLOT = "A1";

    private final ISkuMainDataApi skuMainDataApi;

    private final ISkuBatchAttributeApi skuBatchAttributeApi;

    private final IContainerApi containerApi;

    @Override
    public void beforeDoOperation(OperationContext<OutboundPlanOrderDTO> operationContext) {
        log.info("try create outbound detail bin code and barcode");
        OutboundPlanOrderDTO operationObject = operationContext.getOperationObject();
        if (CollectionUtils.isEmpty(operationObject.getDetails())) {
            log.warn("outbound details is empty");
            return;
        }

        // ownerCode >>> barcode
        Map<String, Set<String>> barcodeGroupByOwnerCodeMap = new HashMap<>();

        // ownerCode + binCode >>> qty
        Map<Pair<String, String>, Integer> echoOwnerSkuBinQtyMap = new HashMap<>();
        operationObject.getDetails().stream()
            .filter(detail -> detail.getReservedFields() != null)
            .forEach(detail -> {
                Map<String, String> reservedFields = detail.getReservedFields();

                // 扩展属性
                if (reservedFields.containsKey(BIN_CODE) && reservedFields.containsKey(BAR_CODE)) {
                    String binCode = reservedFields.get(BIN_CODE);
                    String barcode = reservedFields.get(BAR_CODE);
                    barcodeGroupByOwnerCodeMap.computeIfAbsent(detail.getOwnerCode(), v -> new HashSet<>()).add(barcode);

                    Pair<String, String> keyPair = Pair.of(detail.getOwnerCode(), binCode);
                    Integer qty = echoOwnerSkuBinQtyMap.getOrDefault(keyPair, 0);
                    echoOwnerSkuBinQtyMap.put(keyPair, qty + detail.getQtyRequired());
                }
            });

        if (MapUtils.isEmpty(barcodeGroupByOwnerCodeMap)) {
            log.info("they is no any barcode code sku");
            return;
        }

        List<SkuMainDataDTO> skuMainDataDTOS = barcodeGroupByOwnerCodeMap.entrySet().stream()
            .filter(entry -> CollectionUtils.isNotEmpty(entry.getValue())).map(entry -> {
                String ownerCode = entry.getKey();

                SkuMainDataDTO skuMainDataDTO = new SkuMainDataDTO();
                skuMainDataDTO.setWarehouseCode(operationObject.getWarehouseCode());
                skuMainDataDTO.setOwnerCode(ownerCode);
                skuMainDataDTO.setSkuCode(operationObject.getCustomerOrderNo());
                skuMainDataDTO.setSkuName(operationObject.getCustomerOrderNo());
                skuMainDataDTO.setSkuBarcode(new BarcodeDTO(entry.getValue().stream().toList()));
                return skuMainDataDTO;
            }).toList();

        skuMainDataApi.createOrUpdateBatch(skuMainDataDTOS);
        log.info("create barcode sku success, create size: {}", skuMainDataDTOS.size());


        echoOwnerSkuBinQtyMap.forEach((key, qty) -> {
            String ownerCode = key.getLeft();
            String binCode = key.getRight();
            SkuMainDataDTO skuMainData = skuMainDataApi.getSkuMainData(operationObject.getCustomerOrderNo(), ownerCode);

            if (skuMainData == null) {
                log.warn("cannot find sku info, sku code: {}, ownerCode: {}, binCode: {}", operationObject.getCustomerOrderNo(), ownerCode, binCode);
                return;
            }

            // 批次属性
            Map<String, Object> batchAttribute = new HashMap<>();
            batchAttribute.put(ParserObjectEnum.INBOUND_DATE.getLabel(), DateFormatUtil.getDateFormatYmdNowWithoutLink());

            SkuBatchAttributeDTO skuBatchAttribute = skuBatchAttributeApi.getOrCreateSkuBatchAttribute(skuMainData.getId(), batchAttribute);
            ContainerDTO containerDTO = containerApi.queryContainer(binCode, operationObject.getWarehouseCode());

            StockCreateDTO.StockCreateDTOBuilder stockCreateDTOBuilder = StockCreateDTO.builder();
            stockCreateDTOBuilder.warehouseCode(operationObject.getWarehouseCode());
            stockCreateDTOBuilder.skuBatchAttributeId(skuBatchAttribute.getId());
            stockCreateDTOBuilder.skuId(skuMainData.getId());
            stockCreateDTOBuilder.transferQty(qty);
            stockCreateDTOBuilder.orderNo(operationObject.getCustomerOrderNo());
            stockCreateDTOBuilder.warehouseAreaId(containerDTO.getWarehouseAreaId());
            stockCreateDTOBuilder.sourceContainerCode(operationObject.getCustomerOrderNo());
            stockCreateDTOBuilder.sourceContainerSlotCode(ownerCode);
            stockCreateDTOBuilder.targetContainerId(containerDTO.getId());
            stockCreateDTOBuilder.targetContainerCode(binCode);
            stockCreateDTOBuilder.targetContainerFace(TARGET_CONTAINER_FACE);
            stockCreateDTOBuilder.targetContainerSlotCode(TARGET_CONTAINER_SLOT);
            stockCreateDTOBuilder.boxNo(operationObject.getCustomerOrderNo());
            stockCreateDTOBuilder.boxStock(false);

            StockCreateDTO stockCreateDTO = stockCreateDTOBuilder.build();

            DomainEventPublisher.sendAsyncDomainEvent(new StockCreateEvent(stockCreateDTO));
        });
    }

    @Override
    public void afterDoOperation(OperationContext<OutboundPlanOrderDTO> operationContext) {

    }
}
