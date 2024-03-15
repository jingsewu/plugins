package com.swms.plugins.outbound.sm;

import com.swms.plugin.extend.wms.outbound.IOutboundWavePickingPlugin;
import com.swms.wms.api.outbound.IOutboundPlanOrderApi;
import com.swms.wms.api.outbound.constants.OutboundPlanOrderStatusEnum;
import com.swms.wms.api.outbound.dto.OutboundPlanOrderDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.pf4j.Extension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Extension
@RequiredArgsConstructor
public class LazyOutboundWavePickingPlugin implements IOutboundWavePickingPlugin {

    private final IOutboundPlanOrderApi outboundPlanOrderApi;

    @Override
    public List<List<OutboundPlanOrderDTO>> doOperation(List<OutboundPlanOrderDTO> originalOutboundPlanOrders) {
        log.debug("Receive lazy waving request, original outbound plan orders size: {}", originalOutboundPlanOrders.size());

        Set<String> customerWaveNos = originalOutboundPlanOrders.stream()
            .map(OutboundPlanOrderDTO::getCustomerWaveNo).collect(Collectors.toSet());
        List<OutboundPlanOrderDTO> outboundPlanOrderDTOS = outboundPlanOrderApi.findByCustomerWaveNos(customerWaveNos);

        Map<String, List<OutboundPlanOrderDTO>> outboundPlanOrderGroupMap = outboundPlanOrderDTOS.stream()
            .collect(Collectors.groupingBy(OutboundPlanOrderDTO::getCustomerWaveNo));

        // 复核打包台已上线，或者订单波次没有已完成订单，否则就过滤掉，等待打包台上线
        List<OutboundPlanOrderDTO> outboundPlanOrders = originalOutboundPlanOrders.stream()
            .filter(v -> outboundPlanOrderGroupMap.get(v.getCustomerWaveNo()).stream()
                .noneMatch(dto -> OutboundPlanOrderStatusEnum.NEW != dto.getOutboundPlanOrderStatus()
                    && OutboundPlanOrderStatusEnum.ASSIGNED != dto.getOutboundPlanOrderStatus())
        ).toList();

        log.debug("After filter, new outbound plan orders size: {}", outboundPlanOrders.size());

        // 如果原始单量和过滤后的单量不一致，说明有父单完成了，就需要判断复核打包台的状态了
        if (originalOutboundPlanOrders.size() != outboundPlanOrders.size()) {
            boolean recheckStationIsOnline = recheckStationIsOnline();
            // 复核打包台已上线，就发全部的订单，否则只发父单没有完成的订单
            return waving(recheckStationIsOnline ? originalOutboundPlanOrders : outboundPlanOrders);
        }

        return waving(outboundPlanOrders);
    }

    private boolean recheckStationIsOnline() {
        boolean isOnline = false;
        log.debug("Recheck station online status is : {}", isOnline);
        return isOnline;
    }

    private List<List<OutboundPlanOrderDTO>> waving(List<OutboundPlanOrderDTO> outboundPlanOrders) {
        log.debug("Waving outbound plan orders size: {}", outboundPlanOrders.size());
        // 外部波次号为空的订单单组独成为一个波次
        List<List<OutboundPlanOrderDTO>> emptyWaveNoOrders = outboundPlanOrders.stream()
            .filter(order -> StringUtils.isEmpty(order.getCustomerWaveNo()))
            .map(List::of).toList();

        // 有波次号的订单按照波次号组波
        Map<String, List<OutboundPlanOrderDTO>> outboundWaveMap = outboundPlanOrders.stream()
            .filter(order -> StringUtils.isNotEmpty(order.getCustomerWaveNo()))
            .collect(Collectors.groupingBy(OutboundPlanOrderDTO::getCustomerWaveNo));

        Collection<List<OutboundPlanOrderDTO>> results = CollectionUtils.union(emptyWaveNoOrders, outboundWaveMap.values());
        log.debug("Waving success! empty waving no orders size: {}, has customer wave no plan orders size: {}",
            emptyWaveNoOrders.size(), outboundWaveMap.values().size());
        return new ArrayList<>(results);
    }
}
