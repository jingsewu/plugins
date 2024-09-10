package com.swms.plugins.ems.sm;

import com.swms.api.platform.api.ICallbackApi;
import com.swms.api.platform.api.constants.CallbackApiTypeEnum;
import com.swms.api.platform.api.dto.callback.CallbackMessage;
import com.swms.ems.api.IContainerTaskApi;
import com.swms.ems.api.constants.BusinessTaskTypeEnum;
import com.swms.ems.api.constants.ContainerTaskStatusEnum;
import com.swms.ems.api.constants.ContainerTaskTypeEnum;
import com.swms.ems.api.dto.ContainerTaskAndBusinessTaskRelationDTO;
import com.swms.ems.api.dto.ContainerTaskDTO;
import com.swms.plugin.extend.ems.ContainerTaskCreatePlugin;
import com.swms.wms.api.basic.IContainerApi;
import com.swms.wms.api.basic.ILocationApi;
import com.swms.wms.api.basic.IWorkStationApi;
import com.swms.wms.api.basic.dto.ContainerDTO;
import com.swms.wms.api.basic.dto.LocationDTO;
import com.swms.wms.api.basic.dto.PositionDTO;
import com.swms.wms.api.basic.dto.WorkStationDTO;
import com.swms.wms.api.task.ITaskApi;
import com.swms.wms.api.task.dto.OperationTaskDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Extension
@RequiredArgsConstructor
public class SentrixMobileContainerTaskCreatePlugin implements ContainerTaskCreatePlugin {

    private final ITaskApi taskApi;
    private final IContainerTaskApi containerTaskApi;
    private final IContainerApi containerApi;
    private final ILocationApi locationApi;
    private final IWorkStationApi workStationApi;
    private final ICallbackApi callbackApi;

    @Override
    public void create(List<ContainerTaskDTO> containerTasks, ContainerTaskTypeEnum containerTaskType) {
        ContainerTaskDTO containerTaskDTO = containerTasks.stream().findAny().orElseThrow();
        BusinessTaskTypeEnum businessTaskType = containerTaskDTO.getBusinessTaskType();

        List<Long> newCustomerTaskIds = containerTasks.stream().flatMap(task -> task.getRelations().stream()).map(ContainerTaskAndBusinessTaskRelationDTO::getCustomerTaskId).toList();
        // 非出库搬箱任务，直接回调
        if (!BusinessTaskTypeEnum.PICKING.equals(businessTaskType)) {
            containerTasks.forEach(task -> callback(task, containerTaskType, newCustomerTaskIds));
            return;
        }

        Set<String> destinations = containerTasks.stream().flatMap(task -> task.getDestinations().stream()).collect(Collectors.toSet());

        List<ContainerTaskDTO> allContainerTaskDTOS = containerTaskApi.queryContainerTaskListByDestinations(destinations, ContainerTaskStatusEnum.processingStates, List.of(BusinessTaskTypeEnum.PICKING));

        Set<Long> operationTaskIds = allContainerTaskDTOS.stream()
            .flatMap(task -> task.getRelations().stream()).map(ContainerTaskAndBusinessTaskRelationDTO::getCustomerTaskId).collect(Collectors.toSet());
        List<OperationTaskDTO> allOperationTaskDTOS = taskApi.queryTasks(operationTaskIds);

        String warehouseCode = allOperationTaskDTOS.iterator().next().getWarehouseCode();
        Set<String> containerCodes = allContainerTaskDTOS.stream().map(ContainerTaskDTO::getContainerCode).collect(Collectors.toSet());
        Collection<ContainerDTO> containerDTOS = containerApi.queryContainer(containerCodes, warehouseCode);
        List<LocationDTO> locationDTOS = locationApi.getByShelfCodes(containerCodes);
        List<WorkStationDTO> workStationDTOS = workStationApi.queryWorkStation(destinations.stream().map(Long::valueOf).collect(Collectors.toSet()));

        Map<String, List<ContainerTaskDTO>> containerTaskDTOMap = allContainerTaskDTOS.stream()
            .collect(Collectors.groupingBy(v -> v.getDestinations().iterator().next()));
        Map<String, ContainerDTO> containerDTOMap = containerDTOS.stream().collect(Collectors.toMap(ContainerDTO::getContainerCode, Function.identity()));
        Map<String, LocationDTO> locationDTOMap = locationDTOS.stream().collect(Collectors.toMap(LocationDTO::getLocationCode, Function.identity()));
        Map<Long, WorkStationDTO> workStationDTOMap = workStationDTOS.stream().collect(Collectors.toMap(WorkStationDTO::getId, Function.identity()));

        // 按照工作站对所有搬箱任务进行分组，分别重新排序
        allOperationTaskDTOS.stream()
            .collect(Collectors.groupingBy(v -> v.getAssignedStationSlot().keySet().iterator().next()))
            .forEach((workStationId, operationTaskDTOS) -> {
                List<ContainerTaskDTO> containerTaskDTOS = containerTaskDTOMap.get(String.valueOf(workStationId));

                Map<Long, OperationTaskDTO> operationTaskDTOMap = operationTaskDTOS.stream().collect(Collectors.toMap(OperationTaskDTO::getId, Function.identity()));

                // 所有未完成订单需要的货架
                Map<String, Set<Long>> containerCompleteOrders = operationTaskDTOS.stream()
                    .collect(Collectors.groupingBy(OperationTaskDTO::getSourceContainerCode, Collectors.mapping(OperationTaskDTO::getOrderId, Collectors.toSet())));

                Map<Long, Set<String>> orderRequiredContainers = operationTaskDTOS.stream()
                    .collect(Collectors.groupingBy(OperationTaskDTO::getOrderId, Collectors.mapping(OperationTaskDTO::getSourceContainerCode, Collectors.toSet())));

                // 所有未完成货架可以满足的订单行
                Map<String, Set<Long>> containerCompleteLines = operationTaskDTOS.stream()
                    .collect(Collectors.groupingBy(OperationTaskDTO::getSourceContainerCode, Collectors.mapping(OperationTaskDTO::getDetailId, Collectors.toSet())));

                // 每个货架的任务数
                Map<String, Long> containerTaskSizeMap = containerTaskDTOS.stream()
                    .collect(Collectors.groupingBy(ContainerTaskDTO::getContainerCode, Collectors.counting()));

                Map<Boolean, List<ContainerTaskDTO>> containerTaskMap = containerTaskDTOS.stream()
                    .collect(Collectors.groupingBy(t -> t.getTaskPriority() == null || t.getTaskPriority() == 0));

                // 上游指定了优先级的搬箱任务
                List<ContainerTaskDTO> customerPriorityTasks = containerTaskMap.get(Boolean.FALSE);
                // 上游未指定优先级的搬箱任务
                List<ContainerTaskDTO> noPriorityTasks = containerTaskMap.get(Boolean.TRUE);
                noPriorityTasks.sort((taskA, taskB) -> {
                    // 订单优先级不同，则先判断订单优先级
                    if (!Objects.equals(taskA.getTaskPriority(), taskB.getTaskPriority())) {
                        return taskA.getTaskPriority().compareTo(taskB.getTaskPriority());
                    }

                    String taskAContainerCode = taskA.getContainerCode();
                    String taskBContainerCode = taskB.getContainerCode();
                    // 如果货架号相同，直接返回相等
                    if (taskAContainerCode.equals(taskBContainerCode)) {
                        return 0;
                    }

                    // 释放槽口多的货架优先
                    Set<Long> taskAOrders = containerCompleteOrders.get(taskAContainerCode);
                    Set<Long> taskBOrders = containerCompleteOrders.get(taskBContainerCode);
                    long taskACompleteOrderSize = taskAOrders.stream()
                        .filter(orderId -> orderRequiredContainers.get(orderId).stream().allMatch(c -> c.equals(taskAContainerCode))).count();
                    long taskBCompleteOrderSize = taskBOrders.stream()
                        .filter(orderId -> orderRequiredContainers.get(orderId).stream().allMatch(c -> c.equals(taskBContainerCode))).count();
                    if (taskACompleteOrderSize != taskBCompleteOrderSize) {
                        return taskACompleteOrderSize > taskBCompleteOrderSize ? -1 : 1;
                    }

                    // 满足订单行最多的货架优先
                    Set<Long> taskAOrderLines = containerCompleteLines.get(taskAContainerCode);
                    Set<Long> taskBOrderLines = containerCompleteLines.get(taskBContainerCode);
                    if (taskAOrderLines.size() != taskBOrderLines.size()) {
                        return taskAOrderLines.size() > taskBOrderLines.size() ? -1 : 1;
                    }

                    // 货架任务数最少的货架优先
                    Long taskAContainerTaskCount = containerTaskSizeMap.get(taskAContainerCode);
                    Long taskBContainerTaskCount = containerTaskSizeMap.get(taskBContainerCode);
                    if (!taskAContainerTaskCount.equals(taskBContainerTaskCount)) {
                        return taskBContainerTaskCount.compareTo(taskAContainerTaskCount);
                    }

                    // 按照距离排序
                    ContainerDTO taskAContainerDTO = containerDTOMap.get(taskAContainerCode);
                    ContainerDTO taskBContainerDTO = containerDTOMap.get(taskBContainerCode);
                    LocationDTO taskALocationDTO = locationDTOMap.get(taskAContainerDTO.getLocationCode());
                    LocationDTO taskBLocationDTO = locationDTOMap.get(taskBContainerDTO.getLocationCode());
                    PositionDTO taskAPosition = taskALocationDTO.getPosition();
                    PositionDTO taskBPosition = taskBLocationDTO.getPosition();
                    WorkStationDTO workStationDTO = workStationDTOMap.get(workStationId);
                    PositionDTO workStationPosition = workStationDTO.getPosition();

                    int taskADistance = Math.abs(taskAPosition.getX() - workStationPosition.getX()) + Math.abs(taskAPosition.getY() - workStationPosition.getY());
                    int taskBDistance = Math.abs(taskBPosition.getX() - workStationPosition.getX()) + Math.abs(taskBPosition.getY() - workStationPosition.getY());
                    if (taskADistance != taskBDistance) {
                        return taskADistance > taskBDistance ? -1 : 1;
                    }

                    return 0;
                });

                customerPriorityTasks.forEach(task
                    -> callback(task, containerTaskType, newCustomerTaskIds));

                AtomicInteger priority = new AtomicInteger(1000);
                noPriorityTasks.forEach(task -> {
                    task.setTaskPriority(Math.max(priority.decrementAndGet(), 1));
                    callback(task, containerTaskType, newCustomerTaskIds);
                });
            });
    }

    private void callback(ContainerTaskDTO taskDTO, ContainerTaskTypeEnum bizType, List<Long> newCustomerTaskIds) {
        String bizTypeName = bizType == null ? null : bizType.name();
        List<Long> customerTaskIds = taskDTO.getRelations().stream().map(ContainerTaskAndBusinessTaskRelationDTO::getCustomerTaskId).toList();

        if (customerTaskIds.stream().anyMatch(newCustomerTaskIds::contains)) {
            callbackApi.callback(CallbackApiTypeEnum.CONTAINER_TASK_CREATE, bizTypeName, new CallbackMessage<>().setData(List.of(taskDTO)));
        } else {
            callbackApi.callback(CallbackApiTypeEnum.CONTAINER_TASK_UPDATE, bizTypeName, new CallbackMessage<>().setData(List.of(taskDTO)));
        }
    }
}
