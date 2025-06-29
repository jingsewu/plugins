package com.swms.plugins.ems.sm;

import com.swms.api.platform.api.ICallbackApi;
import com.swms.api.platform.api.constants.CallbackApiTypeEnum;
import com.swms.api.platform.api.dto.callback.CallbackMessage;
import com.swms.ems.api.IContainerTaskApi;
import com.swms.ems.api.constants.BusinessTaskTypeEnum;
import com.swms.ems.api.constants.ContainerTaskAndBusinessTaskRelationStatusEnum;
import com.swms.ems.api.constants.ContainerTaskStatusEnum;
import com.swms.ems.api.constants.ContainerTaskTypeEnum;
import com.swms.ems.api.dto.ContainerOperation;
import com.swms.ems.api.dto.ContainerTaskAndBusinessTaskRelationDTO;
import com.swms.ems.api.dto.ContainerTaskDTO;
import com.swms.ems.api.dto.UpdateContainerTaskDTO;
import com.swms.mdm.api.config.ISystemConfigApi;
import com.swms.mdm.api.config.dto.SystemConfigDTO;
import com.swms.plugin.extend.ems.ContainerTaskCreatePlugin;
import com.swms.wms.api.basic.ILocationApi;
import com.swms.wms.api.basic.IPutWallApi;
import com.swms.wms.api.basic.IWorkStationApi;
import com.swms.wms.api.basic.constants.PutWallSlotStatusEnum;
import com.swms.wms.api.basic.constants.WorkStationStatusEnum;
import com.swms.wms.api.basic.dto.*;
import com.swms.wms.api.outbound.IOutboundPlanOrderApi;
import com.swms.wms.api.outbound.IOutboundWaveApi;
import com.swms.wms.api.outbound.IPickingOrderApi;
import com.swms.wms.api.outbound.constants.PickingOrderStatusEnum;
import com.swms.wms.api.outbound.dto.OutboundWaveDTO;
import com.swms.wms.api.outbound.dto.PickingOrderDTO;
import com.swms.wms.api.task.ITaskApi;
import com.swms.wms.api.task.constants.OperationTaskStatusEnum;
import com.swms.wms.api.task.constants.OperationTaskTypeEnum;
import com.swms.wms.api.task.dto.OperationTaskDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.pf4j.Extension;
import org.springframework.util.StopWatch;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Extension
@RequiredArgsConstructor
public class SentrixMobileContainerTaskCreatePlugin implements ContainerTaskCreatePlugin {

    private final IOutboundPlanOrderApi outboundPlanOrderApi;
    private final IOutboundWaveApi outboundWaveApi;
    private final IPickingOrderApi pickingOrderApi;
    private final ITaskApi taskApi;
    private final IContainerTaskApi containerTaskApi;
    private final ILocationApi locationApi;
    private final IWorkStationApi workStationApi;
    private final ICallbackApi callbackApi;
    private final ISystemConfigApi systemConfigApi;
    private final IPutWallApi putWallApi;

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

        Map<ContainerTaskTypeEnum, List<ContainerTaskDTO>> containerTaskMap = containerTasks.stream()
                .collect(Collectors.groupingBy(ContainerTaskDTO::getContainerTaskType));
        List<ContainerTaskDTO> transferContainerTasks = containerTaskMap.get(ContainerTaskTypeEnum.TRANSFER);
        if (CollectionUtils.isNotEmpty(transferContainerTasks)) {
            transferContainerTasks.forEach(task -> callback(task, containerTaskType, newCustomerTaskIds));
        }

        try {
            List<ContainerTaskDTO> robotPickingContainerTasks = containerTaskMap.get(ContainerTaskTypeEnum.OUTBOUND);
            List<ContainerTaskDTO> robotOutboundContainerTasks = containerTaskMap.get(ContainerTaskTypeEnum.PICKING);
            Collection<ContainerTaskDTO> robotContainerTasks = CollectionUtils.union(robotPickingContainerTasks, robotOutboundContainerTasks);
            if (CollectionUtils.isNotEmpty(robotPickingContainerTasks)) {
                resortContainerTasks(robotContainerTasks, containerTaskType, newCustomerTaskIds);
            }
        } catch (Exception e) {
            log.error("resort robot container tasks failed", e);
            containerTasks.forEach(task -> callback(task, containerTaskType, newCustomerTaskIds));
        }
    }

    @Override
    public void leave(ContainerOperation containerOperation, List<ContainerTaskDTO> containerTasks) {
        ContainerOperation.ContainerOperationDetail container = containerOperation.getContainerOperationDetails().iterator().next();
        // 批量完成所有搬箱任务
        containerTasks.stream().findFirst().ifPresent(task -> {
            List<ContainerOperation.ContainerOperationDetail> details = containerTasks.stream().map(t
                    -> new ContainerOperation.ContainerOperationDetail()
                    .setTaskCode(t.getTaskCode())
                    .setContainerCode(t.getContainerCode())
                    .setContainerFace(t.getContainerFace())
                    .setLocationCode(container.getLocationCode())
                    .setOperationType(container.getOperationType())).toList();

            containerOperation.setContainerOperationDetails(details);
            callbackApi.callback(CallbackApiTypeEnum.CONTAINER_LEAVE, task.getBusinessTaskType().name(), new CallbackMessage<>().setData(containerOperation));
        });

        try {
            Optional<ContainerTaskDTO> containerTaskDTOOpt = containerTasks.stream().findAny();
            containerTaskDTOOpt.ifPresent(containerTaskDTO -> {
                BusinessTaskTypeEnum businessTaskType = containerTaskDTO.getBusinessTaskType();
                // 非出库工作站的货架离开，直接返回
                if (!BusinessTaskTypeEnum.PICKING.equals(businessTaskType)) {
                    return;
                }

                resortContainerTasks(containerTasks, containerTaskDTO.getContainerTaskType(), Collections.emptyList());
            });
        } catch (Exception e) {
            log.error("resort container tasks failed", e);
        }
    }

    private void resortContainerTasks(Collection<ContainerTaskDTO> containerTasks, ContainerTaskTypeEnum containerTaskType, List<Long> newCustomerTaskIds) {
        List<WorkStationDTO> workStationDTOS = workStationApi.queryAllWorkStation().stream()
                .filter(v -> WorkStationStatusEnum.ONLINE == v.getWorkStationStatus())
                .filter(v -> OperationTaskTypeEnum.PICKING == v.getOperationType()).toList();
        List<Long> workingWorkStationIds = workStationDTOS.stream().map(WorkStationDTO::getId).toList();
        List<Long> warehouseAreaIds = workStationDTOS.stream().map(WorkStationDTO::getWarehouseAreaId).toList();

        List<PutWallSlotDTO> idlePutWallSlots = putWallApi.findAllPutWallDTO().stream()
                .filter(PutWallDTO::isEnable)
                .filter(v -> workingWorkStationIds.contains(v.getWorkStationId()))
                .flatMap(v -> v.getPutWallSlots().stream())
                .filter(PutWallSlotDTO::isEnable).filter(v -> PutWallSlotStatusEnum.IDLE == v.getPutWallSlotStatus()).toList();

        List<PickingOrderDTO> unAssignPickingOrders = pickingOrderApi.findByWarehouseAreaIdsAndStatuses(warehouseAreaIds, List.of(PickingOrderStatusEnum.NEW));

        // 空闲槽口不为空，并空闲槽口的数量比新波次数量多
        if (!CollectionUtils.isEmpty(idlePutWallSlots) && idlePutWallSlots.size() >= unAssignPickingOrders.size()) {
            resortContainerTasksForIdle(containerTasks, containerTaskType, newCustomerTaskIds);
        } else {
            resortContainerTasksForBusy(containerTasks, containerTaskType, newCustomerTaskIds);
        }
    }

    private void resortContainerTasksForBusy(Collection<ContainerTaskDTO> containerTasks, ContainerTaskTypeEnum containerTaskType, List<Long> newCustomerTaskIds) {
        StopWatch stopWatch = new StopWatch("sentrix-mobile-container-task-create-plugin-for-busy");
        stopWatch.start("prepare data");
        Set<String> newContainerTaskCodes = containerTasks.stream().map(ContainerTaskDTO::getTaskCode).collect(Collectors.toSet());
        Set<String> destinations = containerTasks.stream().flatMap(task -> task.getDestinations().stream()).collect(Collectors.toSet());

        List<ContainerTaskDTO> allContainerTasks = containerTaskApi.queryContainerTaskListAndExcludeContainerTaskTypes(ContainerTaskStatusEnum.processingStates, List.of(BusinessTaskTypeEnum.PICKING), List.of(ContainerTaskTypeEnum.TRANSFER)).stream()
                // 排除 GO_AHEAD 之类的搬箱任务，避免 GO_AHEAD 的转面任务的优先级被更新
                .filter(v -> v.getContainerTaskType() == ContainerTaskTypeEnum.PICKING || v.getContainerTaskType() == ContainerTaskTypeEnum.OUTBOUND).toList();
        if (CollectionUtils.isEmpty(allContainerTasks)) {
            log.info("All container tasks are completed");
            return;
        }

        SystemConfigDTO.BasicConfigDTO basicConfig = systemConfigApi.get().getBasicConfig();
        Set<String> staticContainerCodes = basicConfig.getStaticContainerConfig().stream()
                .map(SystemConfigDTO.BasicConfigDTO.StaticContainerConfig::getContainerCode).collect(Collectors.toSet());

        List<ContainerTaskDTO> allDestinationContainerTasks = allContainerTasks.stream()
                .filter(task -> task.getDestinations().stream().anyMatch(destinations::contains))
                .toList();

        Set<Long> operationTaskIds = allDestinationContainerTasks.stream()
                .flatMap(task -> task.getRelations().stream()).map(ContainerTaskAndBusinessTaskRelationDTO::getCustomerTaskId).collect(Collectors.toSet());

        if (CollectionUtils.isEmpty(operationTaskIds)) {
            log.info("All operation tasks are completed");
            return;
        }

        List<OperationTaskDTO> allOperationTaskDTOS = taskApi.queryTasks(operationTaskIds).stream()
                .filter(task -> OperationTaskStatusEnum.isStatusNonComplete(task.getTaskStatus())).toList();
        if (CollectionUtils.isEmpty(allOperationTaskDTOS)) {
            log.info("All operation tasks are completed");
            return;
        }

        Set<Long> uncompletedOperationTaskIds = allOperationTaskDTOS.stream().map(OperationTaskDTO::getId).collect(Collectors.toSet());
        allDestinationContainerTasks = allDestinationContainerTasks.stream()
                // 过滤掉静态货架任务
                .filter(task -> !staticContainerCodes.contains(task.getContainerCode()))
                // 过滤掉实际已经完成的搬箱任务
                .filter(task -> task.getRelations().stream().anyMatch(relation -> uncompletedOperationTaskIds.contains(relation.getCustomerTaskId()))).toList();

        Set<Long> pickingOrderIds = allOperationTaskDTOS.stream().map(OperationTaskDTO::getOrderId).collect(Collectors.toSet());
        List<PickingOrderDTO> pickingOrderDTOS = pickingOrderApi.findOrderByPickingOrderIds(pickingOrderIds);
        Set<String> waveNos = pickingOrderDTOS.stream().map(PickingOrderDTO::getWaveNo).collect(Collectors.toSet());
        List<OutboundWaveDTO> waveDTOS = outboundWaveApi.findByWaveNos(waveNos);

        String warehouseCode = pickingOrderDTOS.iterator().next().getWarehouseCode();
        Map<Long, OperationTaskDTO> operationTaskDTOMap = allOperationTaskDTOS.stream().collect(Collectors.toMap(OperationTaskDTO::getId, Function.identity()));
        Map<Long, PickingOrderDTO> pickingOrderDTOMap = pickingOrderDTOS.stream().collect(Collectors.toMap(PickingOrderDTO::getId, Function.identity()));
        Map<String, OutboundWaveDTO> outboundWaveDTOMap = waveDTOS.stream().collect(Collectors.toMap(OutboundWaveDTO::getWaveNo, Function.identity()));

        Map<String, Optional<Integer>> containerOrderPriorityMap = allDestinationContainerTasks.stream()
                .collect(Collectors.groupingBy(ContainerTaskDTO::getContainerCode, Collectors.flatMapping(task -> task.getRelations().stream()
                        .filter(v -> operationTaskDTOMap.containsKey(v.getCustomerTaskId()))
                        .filter(v -> ContainerTaskAndBusinessTaskRelationStatusEnum.processingStates.contains(v.getContainerTaskAndBusinessTaskRelationStatus()))
                        .map(r -> {
                            int wavePriority = outboundWaveDTOMap.get(pickingOrderDTOMap.get(operationTaskDTOMap.get(r.getCustomerTaskId()).getOrderId()).getWaveNo()).getPriority();
                            Integer taskPriority = operationTaskDTOMap.get(r.getCustomerTaskId()).getPriority();
                            return Math.max(wavePriority, taskPriority);
                        }), Collectors.maxBy(Integer::compareTo))));

        Set<String> containerCodes = allDestinationContainerTasks.stream().map(ContainerTaskDTO::getContainerCode).collect(Collectors.toSet());
        List<LocationDTO> locationDTOS = locationApi.getByShelfCodes(containerCodes, warehouseCode);
        List<WorkStationDTO> workStationDTOS = workStationApi.queryWorkStation(destinations.stream().map(Long::valueOf).collect(Collectors.toSet()));

        Map<String, List<ContainerTaskDTO>> containerTaskDTOMap = allDestinationContainerTasks.stream()
                .collect(Collectors.groupingBy(v -> v.getDestinations().iterator().next()));
        Map<String, LocationDTO> locationDTOMap = locationDTOS.stream().collect(Collectors.toMap(LocationDTO::getShelfCode, Function.identity()));
        Map<Long, WorkStationDTO> workStationDTOMap = workStationDTOS.stream().collect(Collectors.toMap(WorkStationDTO::getId, Function.identity()));

        // 每个货架的目标工作站列表
        Map<String, Set<String>> containerTaskDestinationSizeMap = allContainerTasks.stream()
                .collect(Collectors.groupingBy(ContainerTaskDTO::getContainerCode, Collectors.flatMapping(t -> t.getDestinations().stream(), Collectors.toSet())));
        stopWatch.stop();

        stopWatch.start("First sort all tasks");
        List<ContainerTaskDTO> priorityChangedTasks = new ArrayList<>();
        // 按照工作站对所有搬箱任务进行分组，分别重新排序
        allOperationTaskDTOS.stream()
                .filter(v -> containerTaskDTOMap.containsKey(String.valueOf(v.getAssignedStationSlot().keySet().iterator().next())))
                .collect(Collectors.groupingBy(v -> v.getAssignedStationSlot().keySet().iterator().next()))
                .forEach((workStationId, operationTaskDTOS) -> {
                    List<ContainerTaskDTO> containerTaskDTOS = containerTaskDTOMap.get(String.valueOf(workStationId));

                    // 所有未完成订单需要的货架
                    Map<String, Set<Long>> containerCompleteOrders = operationTaskDTOS.stream()
                            .collect(Collectors.groupingBy(OperationTaskDTO::getSourceContainerCode, Collectors.mapping(OperationTaskDTO::getOrderId, Collectors.toSet())));

                    Map<Long, Set<String>> orderRequiredContainers = operationTaskDTOS.stream()
                            .collect(Collectors.groupingBy(OperationTaskDTO::getOrderId, Collectors.mapping(OperationTaskDTO::getSourceContainerCode, Collectors.toSet())));

                    // 所有未完成货架可以满足的订单行
                    Map<String, Set<Long>> containerCompleteLines = operationTaskDTOS.stream()
                            .collect(Collectors.groupingBy(OperationTaskDTO::getSourceContainerCode, Collectors.mapping(OperationTaskDTO::getDetailId, Collectors.toSet())));

                    Map<Boolean, List<ContainerTaskDTO>> containerTaskMap = containerTaskDTOS.stream()
                            .collect(Collectors.groupingBy(t -> containerOrderPriorityMap.get(t.getContainerCode()).isEmpty()
                                    || containerOrderPriorityMap.get(t.getContainerCode()).get() == 0));

                    // 上游未指定优先级的搬箱任务
                    List<ContainerTaskDTO> noPriorityTasks = containerTaskMap.get(Boolean.TRUE);
                    if (!CollectionUtils.isEmpty(noPriorityTasks)) {
                        noPriorityTasks.sort((taskA, taskB) -> {
                            String taskAContainerCode = taskA.getContainerCode();
                            String taskBContainerCode = taskB.getContainerCode();

                            // 如果货架号相同，直接返回相等
                            if (taskAContainerCode.equals(taskBContainerCode)) {
                                log.debug("taskA {} and taskB {} has the same container code {}", taskA.getId(), taskB.getId(), taskAContainerCode);
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
                                log.debug("taskA {} container code {} complete order size {} and taskB {} container code {} complete order size {}",
                                        taskA.getId(), taskAContainerCode, taskACompleteOrderSize, taskB.getId(), taskBContainerCode, taskBCompleteOrderSize);
                                return taskACompleteOrderSize > taskBCompleteOrderSize ? -1 : 1;
                            }

                            // 满足订单行最多的货架优先
                            Set<Long> taskAOrderLines = containerCompleteLines.get(taskAContainerCode);
                            Set<Long> taskBOrderLines = containerCompleteLines.get(taskBContainerCode);
                            if (taskAOrderLines.size() != taskBOrderLines.size()) {
                                log.debug("taskA {} container code {} order line size {} and taskB {} container code {} order line size {}",
                                        taskA.getId(), taskAContainerCode, taskAOrderLines.size(), taskB.getId(), taskBContainerCode, taskBOrderLines.size());
                                return taskAOrderLines.size() > taskBOrderLines.size() ? -1 : 1;
                            }

                            // 货架任务数最少的货架优先
                            Integer taskAContainerTaskDestinationCount = containerTaskDestinationSizeMap.get(taskAContainerCode).size();
                            Integer taskBContainerTaskDestinationCount = containerTaskDestinationSizeMap.get(taskBContainerCode).size();
                            if (!taskAContainerTaskDestinationCount.equals(taskBContainerTaskDestinationCount)) {
                                log.debug("taskA {} container code {} container destination count {} and taskB {} container code {} container destination count {}",
                                        taskA.getId(), taskAContainerCode, taskAContainerTaskDestinationCount, taskB.getId(), taskBContainerCode, taskBContainerTaskDestinationCount);
                                return taskAContainerTaskDestinationCount.compareTo(taskBContainerTaskDestinationCount);
                            }

                            // 按照距离排序
                            LocationDTO taskALocationDTO = locationDTOMap.get(taskAContainerCode);
                            LocationDTO taskBLocationDTO = locationDTOMap.get(taskBContainerCode);
                            PositionDTO taskAPosition = taskALocationDTO == null ? PositionDTO.UNKNOWN_POSITION : taskALocationDTO.getPosition();
                            PositionDTO taskBPosition = taskBLocationDTO == null ? PositionDTO.UNKNOWN_POSITION : taskBLocationDTO.getPosition();
                            WorkStationDTO workStationDTO = workStationDTOMap.get(workStationId);
                            PositionDTO workStationPosition = workStationDTO.getPosition();

                            int taskADistance = Math.abs(taskAPosition.getX() - workStationPosition.getX()) + Math.abs(taskAPosition.getY() - workStationPosition.getY());
                            int taskBDistance = Math.abs(taskBPosition.getX() - workStationPosition.getX()) + Math.abs(taskBPosition.getY() - workStationPosition.getY());
                            if (taskADistance != taskBDistance) {
                                log.debug("The both task has different distance to workstation {}, taskA {} container code {} distance {} and taskB {} container code {} distance {}",
                                        workStationDTO.getStationCode(), taskA.getId(), taskAContainerCode, taskADistance, taskB.getId(), taskBContainerCode, taskBDistance);
                                return taskADistance < taskBDistance ? -1 : 1;
                            }

                            return 0;
                        });
                    }

                    // 上游指定了优先级的搬箱任务
                    List<ContainerTaskDTO> customerPriorityTasks = containerTaskMap.get(Boolean.FALSE);
                    if (!CollectionUtils.isEmpty(customerPriorityTasks)) {
                        customerPriorityTasks.forEach(task -> {

                            Optional<Integer> priority = containerOrderPriorityMap.get(task.getContainerCode());
                            priority.ifPresent(value -> {
                                if (!Objects.equals(task.getTaskPriority(), value)) {
                                    task.setTaskPriority(value);
                                    priorityChangedTasks.add(task);
                                } else if (newContainerTaskCodes.contains(task.getTaskCode())) {
                                    priorityChangedTasks.add(task);
                                }
                            });
                        });
                    }

                    if (!CollectionUtils.isEmpty(noPriorityTasks)) {
                        AtomicInteger priority = new AtomicInteger(1000);

                        Iterator<ContainerTaskDTO> iterator = noPriorityTasks.iterator();

                        int currentPriority = 0;
                        String lastContainerCode = null;

                        // 标记是否添加过，避免 997 的任务重复添加
                        boolean addedFlag;
                        while (iterator.hasNext()) {
                            ContainerTaskDTO task = iterator.next();
                            addedFlag = false;

                            if (!Objects.equals(lastContainerCode, task.getContainerCode())) {
                                lastContainerCode = task.getContainerCode();
                                currentPriority = priority.decrementAndGet();
                            }

                            // 跳过 997，留给最后一个任务，避免尾波时间太长
                            if (currentPriority == 997 && iterator.hasNext()) {
                                currentPriority = priority.decrementAndGet();
                            }

                            if (task.getTaskPriority() != currentPriority) {
                                task.setTaskPriority(currentPriority);
                                priorityChangedTasks.add(task);
                                addedFlag = true;
                            }
                            if (!iterator.hasNext() && currentPriority < 997 && task.getTaskPriority() != 997) {
                                task.setTaskPriority(997);
                                if (!addedFlag) {
                                    priorityChangedTasks.add(task);
                                }
                            }
                        }
                    }
                });
        stopWatch.stop();

        stopWatch.start("Second sort priority changed tasks and send to RCS");
        // 所有工作站的任务计算完优先级后，再倒序排序后，按顺序发送给 RCS
        priorityChangedTasks.stream()
                .sorted((taskA, taskB) -> taskB.getTaskPriority().compareTo(taskA.getTaskPriority()))
                // 只发状态为 NEW，PROCESSING 的说明 RCS 已经在执行了，更新优先级已经没有意义了
                .filter(v -> ContainerTaskStatusEnum.NEW == v.getTaskStatus())
                .forEach(task -> callback(task, containerTaskType, newCustomerTaskIds));
        stopWatch.stop();

        stopWatch.start("Save priority changed tasks to database");
        // 记录新的优先级
        List<UpdateContainerTaskDTO> updateContainerTaskDTOS = priorityChangedTasks.stream().map(task -> {
            UpdateContainerTaskDTO updateContainerTaskDTO = new UpdateContainerTaskDTO();
            updateContainerTaskDTO.setTaskCode(task.getTaskCode());
            updateContainerTaskDTO.setTaskPriority(task.getTaskPriority());
            return updateContainerTaskDTO;
        }).toList();
        containerTaskApi.updateContainerTaskPriority(updateContainerTaskDTOS);
        stopWatch.stop();

        log.debug("Total cost info: {}", stopWatch.prettyPrint());
    }

    private void resortContainerTasksForIdle(Collection<ContainerTaskDTO> containerTasks, ContainerTaskTypeEnum containerTaskType, List<Long> newCustomerTaskIds) {
        StopWatch stopWatch = new StopWatch("sentrix-mobile-container-task-create-plugin");
        stopWatch.start("prepare data");
        Set<String> newContainerTaskCodes = containerTasks.stream().map(ContainerTaskDTO::getTaskCode).collect(Collectors.toSet());
        Set<String> destinations = containerTasks.stream().flatMap(task -> task.getDestinations().stream()).collect(Collectors.toSet());

        List<ContainerTaskDTO> allContainerTasks = containerTaskApi.queryContainerTaskListAndExcludeContainerTaskTypes(ContainerTaskStatusEnum.processingStates, List.of(BusinessTaskTypeEnum.PICKING), List.of(ContainerTaskTypeEnum.TRANSFER)).stream()
                // 排除 GO_AHEAD 之类的搬箱任务，避免 GO_AHEAD 的转面任务的优先级被更新
                .filter(v -> v.getContainerTaskType() == ContainerTaskTypeEnum.PICKING || v.getContainerTaskType() == ContainerTaskTypeEnum.OUTBOUND).toList();
        if (CollectionUtils.isEmpty(allContainerTasks)) {
            log.info("All container tasks are completed");
            return;
        }

        SystemConfigDTO.BasicConfigDTO basicConfig = systemConfigApi.get().getBasicConfig();
        Set<String> staticContainerCodes = basicConfig.getStaticContainerConfig().stream()
                .map(SystemConfigDTO.BasicConfigDTO.StaticContainerConfig::getContainerCode).collect(Collectors.toSet());

        List<ContainerTaskDTO> allDestinationContainerTasks = allContainerTasks.stream()
                .filter(task -> !staticContainerCodes.contains(task.getContainerCode()) && task.getDestinations().stream().anyMatch(destinations::contains))
                .toList();

        Set<Long> operationTaskIds = allDestinationContainerTasks.stream()
                .flatMap(task -> task.getRelations().stream()).map(ContainerTaskAndBusinessTaskRelationDTO::getCustomerTaskId).collect(Collectors.toSet());

        if (CollectionUtils.isEmpty(operationTaskIds)) {
            log.info("All operation tasks are completed");
            return;
        }

        List<OperationTaskDTO> allOperationTaskDTOS = taskApi.queryTasks(operationTaskIds).stream()
                .filter(task -> OperationTaskStatusEnum.isStatusNonComplete(task.getTaskStatus())).toList();
        if (CollectionUtils.isEmpty(allOperationTaskDTOS)) {
            log.info("All operation tasks are completed");
            return;
        }

        Set<Long> uncompletedOperationTaskIds = allOperationTaskDTOS.stream().map(OperationTaskDTO::getId).collect(Collectors.toSet());
        // 过滤掉实际已经完成的搬箱任务
        allDestinationContainerTasks = allDestinationContainerTasks.stream()
                .filter(task -> task.getRelations().stream().anyMatch(relation -> uncompletedOperationTaskIds.contains(relation.getCustomerTaskId()))).toList();

        Set<Long> pickingOrderIds = allOperationTaskDTOS.stream().map(OperationTaskDTO::getOrderId).collect(Collectors.toSet());
        List<PickingOrderDTO> pickingOrderDTOS = pickingOrderApi.findOrderByPickingOrderIds(pickingOrderIds);
        Set<String> waveNos = pickingOrderDTOS.stream().map(PickingOrderDTO::getWaveNo).collect(Collectors.toSet());
        List<OutboundWaveDTO> waveDTOS = outboundWaveApi.findByWaveNos(waveNos);

        String warehouseCode = pickingOrderDTOS.iterator().next().getWarehouseCode();
        Map<Long, OperationTaskDTO> operationTaskDTOMap = allOperationTaskDTOS.stream().collect(Collectors.toMap(OperationTaskDTO::getId, Function.identity()));
        Map<Long, PickingOrderDTO> pickingOrderDTOMap = pickingOrderDTOS.stream().collect(Collectors.toMap(PickingOrderDTO::getId, Function.identity()));
        Map<String, OutboundWaveDTO> outboundWaveDTOMap = waveDTOS.stream().collect(Collectors.toMap(OutboundWaveDTO::getWaveNo, Function.identity()));

        Map<String, Optional<Integer>> containerOrderPriorityMap = allDestinationContainerTasks.stream()
                .collect(Collectors.groupingBy(ContainerTaskDTO::getContainerCode, Collectors.flatMapping(task -> task.getRelations().stream()
                        .filter(v -> operationTaskDTOMap.containsKey(v.getCustomerTaskId()))
                        .filter(v -> ContainerTaskAndBusinessTaskRelationStatusEnum.processingStates.contains(v.getContainerTaskAndBusinessTaskRelationStatus()))
                        .map(r -> {
                            int wavePriority = outboundWaveDTOMap.get(pickingOrderDTOMap.get(operationTaskDTOMap.get(r.getCustomerTaskId()).getOrderId()).getWaveNo()).getPriority();
                            Integer taskPriority = operationTaskDTOMap.get(r.getCustomerTaskId()).getPriority();
                            return Math.max(wavePriority, taskPriority);
                        }), Collectors.maxBy(Integer::compareTo))));

        Set<String> containerCodes = allDestinationContainerTasks.stream().map(ContainerTaskDTO::getContainerCode).collect(Collectors.toSet());
        List<LocationDTO> locationDTOS = locationApi.getByShelfCodes(containerCodes, warehouseCode);
        List<WorkStationDTO> workStationDTOS = workStationApi.queryWorkStation(destinations.stream().map(Long::valueOf).collect(Collectors.toSet()));

        Map<String, List<ContainerTaskDTO>> containerTaskDTOMap = allDestinationContainerTasks.stream()
                .collect(Collectors.groupingBy(v -> v.getDestinations().iterator().next()));
        Map<String, LocationDTO> locationDTOMap = locationDTOS.stream().collect(Collectors.toMap(LocationDTO::getShelfCode, Function.identity()));
        Map<Long, WorkStationDTO> workStationDTOMap = workStationDTOS.stream().collect(Collectors.toMap(WorkStationDTO::getId, Function.identity()));

        // 每个货架的目标工作站列表
        Map<String, Set<String>> containerTaskDestinationSizeMap = allContainerTasks.stream()
                .collect(Collectors.groupingBy(ContainerTaskDTO::getContainerCode, Collectors.flatMapping(t -> t.getDestinations().stream(), Collectors.toSet())));
        stopWatch.stop();

        stopWatch.start("First sort all tasks");
        List<ContainerTaskDTO> priorityChangedTasks = new ArrayList<>();
        // 按照工作站对所有搬箱任务进行分组，分别重新排序
        allOperationTaskDTOS.stream()
                .filter(v -> containerTaskDTOMap.containsKey(String.valueOf(v.getAssignedStationSlot().keySet().iterator().next())))
                .collect(Collectors.groupingBy(v -> v.getAssignedStationSlot().keySet().iterator().next()))
                .forEach((workStationId, operationTaskDTOS) -> {
                    List<ContainerTaskDTO> containerTaskDTOS = containerTaskDTOMap.get(String.valueOf(workStationId));

                    // 所有未完成货架可以满足的订单行
                    Map<String, Set<Long>> containerCompleteLines = operationTaskDTOS.stream()
                            .collect(Collectors.groupingBy(OperationTaskDTO::getSourceContainerCode, Collectors.mapping(OperationTaskDTO::getDetailId, Collectors.toSet())));

                    Map<Boolean, List<ContainerTaskDTO>> containerTaskMap = containerTaskDTOS.stream()
                            .collect(Collectors.groupingBy(t -> containerOrderPriorityMap.get(t.getContainerCode()).isEmpty()
                                    || containerOrderPriorityMap.get(t.getContainerCode()).get() == 0));

                    // 上游未指定优先级的搬箱任务
                    List<ContainerTaskDTO> noPriorityTasks = containerTaskMap.get(Boolean.TRUE);
                    if (!CollectionUtils.isEmpty(noPriorityTasks)) {
                        noPriorityTasks.sort((taskA, taskB) -> {
                            String taskAContainerCode = taskA.getContainerCode();
                            String taskBContainerCode = taskB.getContainerCode();

                            // 如果货架号相同，直接返回相等
                            if (taskAContainerCode.equals(taskBContainerCode)) {
                                log.debug("taskA {} and taskB {} has the same container code {}", taskA.getId(), taskB.getId(), taskAContainerCode);
                                return 0;
                            }

                            // 满足订单行最多的货架优先
                            Set<Long> taskAOrderLines = containerCompleteLines.get(taskAContainerCode);
                            Set<Long> taskBOrderLines = containerCompleteLines.get(taskBContainerCode);
                            if (taskAOrderLines.size() != taskBOrderLines.size()) {
                                log.debug("taskA {} container code {} order line size {} and taskB {} container code {} order line size {}",
                                        taskA.getId(), taskAContainerCode, taskAOrderLines.size(), taskB.getId(), taskBContainerCode, taskBOrderLines.size());
                                return taskAOrderLines.size() > taskBOrderLines.size() ? -1 : 1;
                            }

                            // 货架任务数最少的货架优先
                            Integer taskAContainerTaskDestinationCount = containerTaskDestinationSizeMap.get(taskAContainerCode).size();
                            Integer taskBContainerTaskDestinationCount = containerTaskDestinationSizeMap.get(taskBContainerCode).size();
                            if (!taskAContainerTaskDestinationCount.equals(taskBContainerTaskDestinationCount)) {
                                log.debug("taskA {} container code {} container destination count {} and taskB {} container code {} container destination count {}",
                                        taskA.getId(), taskAContainerCode, taskAContainerTaskDestinationCount, taskB.getId(), taskBContainerCode, taskBContainerTaskDestinationCount);
                                return taskAContainerTaskDestinationCount.compareTo(taskBContainerTaskDestinationCount);
                            }

                            // 按照距离排序
                            LocationDTO taskALocationDTO = locationDTOMap.get(taskAContainerCode);
                            LocationDTO taskBLocationDTO = locationDTOMap.get(taskBContainerCode);
                            PositionDTO taskAPosition = taskALocationDTO == null ? PositionDTO.UNKNOWN_POSITION : taskALocationDTO.getPosition();
                            PositionDTO taskBPosition = taskBLocationDTO == null ? PositionDTO.UNKNOWN_POSITION : taskBLocationDTO.getPosition();
                            WorkStationDTO workStationDTO = workStationDTOMap.get(workStationId);
                            PositionDTO workStationPosition = workStationDTO.getPosition();

                            int taskADistance = Math.abs(taskAPosition.getX() - workStationPosition.getX()) + Math.abs(taskAPosition.getY() - workStationPosition.getY());
                            int taskBDistance = Math.abs(taskBPosition.getX() - workStationPosition.getX()) + Math.abs(taskBPosition.getY() - workStationPosition.getY());
                            if (taskADistance != taskBDistance) {
                                log.debug("The both task has different distance to workstation {}, taskA {} container code {} distance {} and taskB {} container code {} distance {}",
                                        workStationDTO.getStationCode(), taskA.getId(), taskAContainerCode, taskADistance, taskB.getId(), taskBContainerCode, taskBDistance);
                                return taskADistance < taskBDistance ? -1 : 1;
                            }

                            return 0;
                        });
                    }

                    // 上游指定了优先级的搬箱任务
                    List<ContainerTaskDTO> customerPriorityTasks = containerTaskMap.get(Boolean.FALSE);
                    if (!CollectionUtils.isEmpty(customerPriorityTasks)) {
                        customerPriorityTasks.forEach(task -> {

                            Optional<Integer> priority = containerOrderPriorityMap.get(task.getContainerCode());
                            priority.ifPresent(value -> {
                                if (!Objects.equals(task.getTaskPriority(), value)) {
                                    task.setTaskPriority(value);
                                    priorityChangedTasks.add(task);
                                } else if (newContainerTaskCodes.contains(task.getTaskCode())) {
                                    priorityChangedTasks.add(task);
                                }
                            });
                        });
                    }

                    if (!CollectionUtils.isEmpty(noPriorityTasks)) {
                        AtomicInteger priority = new AtomicInteger(1000);

                        Iterator<ContainerTaskDTO> iterator = noPriorityTasks.iterator();

                        int currentPriority = 0;
                        String lastContainerCode = null;

                        // 标记是否添加过，避免 997 的任务重复添加
                        boolean addedFlag;
                        while (iterator.hasNext()) {
                            ContainerTaskDTO task = iterator.next();
                            addedFlag = false;

                            if (!Objects.equals(lastContainerCode, task.getContainerCode())) {
                                lastContainerCode = task.getContainerCode();
                                currentPriority = priority.decrementAndGet();
                            }

                            // 跳过 997，留给最后一个任务，避免尾波时间太长
                            if (currentPriority == 997 && iterator.hasNext()) {
                                currentPriority = priority.decrementAndGet();
                            }

                            if (task.getTaskPriority() != currentPriority) {
                                task.setTaskPriority(currentPriority);
                                priorityChangedTasks.add(task);
                                addedFlag = true;
                            }
                            if (!iterator.hasNext() && currentPriority < 997 && task.getTaskPriority() != 997) {
                                task.setTaskPriority(997);
                                if (!addedFlag) {
                                    priorityChangedTasks.add(task);
                                }
                            }
                        }
                    }
                });
        stopWatch.stop();

        stopWatch.start("Second sort priority changed tasks and send to RCS");
        // 所有工作站的任务计算完优先级后，再倒序排序后，按顺序发送给 RCS
        priorityChangedTasks.stream()
                .sorted((taskA, taskB) -> taskB.getTaskPriority().compareTo(taskA.getTaskPriority()))
                // 只发状态为 NEW，PROCESSING 的说明 RCS 已经在执行了，更新优先级已经没有意义了
                .filter(v -> ContainerTaskStatusEnum.NEW == v.getTaskStatus())
                .forEach(task -> callback(task, containerTaskType, newCustomerTaskIds));
        stopWatch.stop();

        stopWatch.start("Save priority changed tasks to database");
        // 记录新的优先级
        List<UpdateContainerTaskDTO> updateContainerTaskDTOS = priorityChangedTasks.stream().map(task -> {
            UpdateContainerTaskDTO updateContainerTaskDTO = new UpdateContainerTaskDTO();
            updateContainerTaskDTO.setTaskCode(task.getTaskCode());
            updateContainerTaskDTO.setTaskPriority(task.getTaskPriority());
            return updateContainerTaskDTO;
        }).toList();
        containerTaskApi.updateContainerTaskPriority(updateContainerTaskDTOS);
        stopWatch.stop();

        log.debug("Total cost info: {}", stopWatch.prettyPrint());
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
