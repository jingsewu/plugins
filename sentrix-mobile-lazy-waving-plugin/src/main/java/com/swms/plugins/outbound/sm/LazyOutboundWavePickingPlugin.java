package com.swms.plugins.outbound.sm;

import com.swms.plugin.extend.wms.outbound.IOutboundWavePickingPlugin;
import com.swms.wms.api.outbound.IOutboundPlanOrderApi;
import com.swms.wms.api.outbound.constants.OutboundPlanOrderStatusEnum;
import com.swms.wms.api.outbound.dto.OutboundPlanOrderDTO;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.pf4j.Extension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Extension
@RequiredArgsConstructor
public class LazyOutboundWavePickingPlugin implements IOutboundWavePickingPlugin {

    private final IOutboundPlanOrderApi outboundPlanOrderApi;

    @Override
    public List<List<OutboundPlanOrderDTO>> doOperation(List<OutboundPlanOrderDTO> originalOutboundPlanOrders) {
        Set<String> customerWaveNos = originalOutboundPlanOrders.stream()
            .map(OutboundPlanOrderDTO::getCustomerWaveNo).collect(Collectors.toSet());
        List<OutboundPlanOrderDTO> outboundPlanOrderDTOS = outboundPlanOrderApi.findByCustomerWaveNos(customerWaveNos);

        Map<String, List<OutboundPlanOrderDTO>> outboundPlanOrderGroupMap = outboundPlanOrderDTOS.stream()
            .collect(Collectors.groupingBy(OutboundPlanOrderDTO::getCustomerWaveNo));

        boolean recheckStationIsOnline = recheckStationIsOnline();

        // 复核打包台已上线，或者订单波次没有已完成订单，否则就过滤掉，等待打包台上线
        List<OutboundPlanOrderDTO> outboundPlanOrders = originalOutboundPlanOrders.stream().filter(v ->
            recheckStationIsOnline || outboundPlanOrderGroupMap.get(v.getCustomerWaveNo()).stream()
                .noneMatch(dto -> OutboundPlanOrderStatusEnum.isFinalStatues(dto.getOutboundPlanOrderStatus()))
        ).toList();

        // 外部波次号为空的订单单组独成为一个波次
        List<List<OutboundPlanOrderDTO>> emptyWaveNoOrders = outboundPlanOrders.stream()
            .filter(order -> StringUtils.isEmpty(order.getCustomerWaveNo()))
            .map(List::of).toList();

        // 有波次号的订单按照波次号组波
        Map<String, List<OutboundPlanOrderDTO>> outboundWaveMap = outboundPlanOrders.stream()
            .filter(order -> StringUtils.isNotEmpty(order.getCustomerWaveNo()))
            .collect(Collectors.groupingBy(OutboundPlanOrderDTO::getCustomerWaveNo));

        Collection<List<OutboundPlanOrderDTO>> results = CollectionUtils.union(emptyWaveNoOrders, outboundWaveMap.values());
        return new ArrayList<>(results);
    }

    private boolean recheckStationIsOnline() {
        return false;
    }
}
