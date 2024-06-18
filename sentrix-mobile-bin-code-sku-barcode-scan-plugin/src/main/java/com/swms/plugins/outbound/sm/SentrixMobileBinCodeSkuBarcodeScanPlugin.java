package com.swms.plugins.outbound.sm;

import com.swms.common.utils.exception.WmsException;
import com.swms.common.utils.exception.code_enum.OperationTaskErrorDescEnum;
import com.swms.common.utils.user.UserContext;
import com.swms.mdm.api.config.constants.ExecuteTimeEnum;
import com.swms.mdm.api.config.constants.UnionLocationEnum;
import com.swms.mdm.api.config.dto.BarcodeParseRequestDTO;
import com.swms.mdm.api.config.dto.BarcodeParseResult;
import com.swms.mdm.api.config.dto.BarcodeParseRuleDTO;
import com.swms.mdm.api.main.data.ISkuMainDataApi;
import com.swms.mdm.api.main.data.dto.SkuMainDataDTO;
import com.swms.plugin.extend.extensions.OperationContext;
import com.swms.plugin.extend.mdm.config.IBarcodeParsePlugin;
import com.swms.user.api.UserApi;
import com.swms.user.api.dto.UserDTO;
import com.swms.wms.api.outbound.IPickingOrderApi;
import com.swms.wms.api.outbound.dto.PickingOrderDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.pf4j.Extension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Extension
@RequiredArgsConstructor
public class SentrixMobileBinCodeSkuBarcodeScanPlugin implements IBarcodeParsePlugin {

    private final UserApi userApi;
    private final IPickingOrderApi pickingOrderApi;
    private final ISkuMainDataApi skuMainDataApi;

    @Override
    public List<BarcodeParseResult> doOperation(OperationContext<BarcodeParsePluginRequest> operationContext) {
        BarcodeParsePluginRequest request = operationContext.getOperationObject();
        BarcodeParseRequestDTO barcodeParseRequestDTO = request.getBarcodeParseRequestDTO();
        List<BarcodeParseRuleDTO> barcodeParseRules = request.getBarcodeParseRules();

        // 如果扫描的不是 SKU，直接返回
        if (!ExecuteTimeEnum.SCAN_SKU.equals(barcodeParseRequestDTO.getExecuteTime())) {
            return defaultParse(barcodeParseRequestDTO, barcodeParseRules);
        }

        String currentUser = UserContext.getCurrentUser();
        if (currentUser == null) {
            log.warn("cannot find current user");
            return defaultParse(barcodeParseRequestDTO, barcodeParseRules);
        }
        UserDTO user = userApi.getByUsername(currentUser);
        if (user == null) {
            log.warn("cannot find current user by username : {}", currentUser);
            return defaultParse(barcodeParseRequestDTO, barcodeParseRules);
        }

        List<PickingOrderDTO> pickingOrderDTOS = pickingOrderApi.findUncompletedByReceivedUserId(user.getId());
        if (CollectionUtils.isEmpty(pickingOrderDTOS)) {
            log.info("maybe robot area picking order");
            return defaultParse(barcodeParseRequestDTO, barcodeParseRules);
        }
        Set<Long> skuIds = pickingOrderDTOS.stream().flatMap(v -> v.getDetails().stream()
            .map(PickingOrderDTO.PickingOrderDetailDTO::getSkuId)).collect(Collectors.toSet());

        List<SkuMainDataDTO> skuMainDataDTOS = skuMainDataApi.getByIds(skuIds);
        Set<String> barcodes = skuMainDataDTOS.stream()
            .flatMap(v -> v.getSkuBarcode().getBarcodes().stream()).collect(Collectors.toSet());
        if (CollectionUtils.containsAny(barcodes, barcodeParseRequestDTO.getBarcode())) {
            return defaultParse(barcodeParseRequestDTO, barcodeParseRules);
        }

        throw WmsException.throwWmsException(OperationTaskErrorDescEnum.INCRRECT_BAR_CODE);
    }

    private List<BarcodeParseResult> defaultParse(BarcodeParseRequestDTO barcodeParseRequestDTO, List<BarcodeParseRuleDTO> barcodeParseRules) {
        for (BarcodeParseRuleDTO parseRule : barcodeParseRules) {
            List<BarcodeParseResult> results = parse(barcodeParseRequestDTO.getBarcode(), parseRule);
            if (CollectionUtils.isNotEmpty(results)) {
                return results;
            }
        }
        return new ArrayList<>();
    }

    public List<BarcodeParseResult> parse(String barcode, BarcodeParseRuleDTO parseRule) {
        String unionBarcode = union(barcode, parseRule);
        List<String> compileResult = compile(unionBarcode, parseRule);
        return buildResult(compileResult, parseRule);
    }

    private String union(String barcode, BarcodeParseRuleDTO parseRule) {
        String unionStr = parseRule.getUnionStr();
        if (parseRule.getUnionLocation() == UnionLocationEnum.LEFT) {
            return StringUtils.join(unionStr, barcode);
        } else {
            return StringUtils.join(barcode, unionStr);
        }
    }

    private List<String> compile(String unionBarcode, BarcodeParseRuleDTO parseRule) {
        Pattern pattern = Pattern.compile(parseRule.getRegularExpression());
        Matcher matcher = pattern.matcher(unionBarcode);
        List<String> result = new ArrayList<>(parseRule.getResultFields().size());
        if (matcher.find()) {
            try {
                for (int i = 1; i <= parseRule.getResultFields().size(); i++) {
                    result.add(matcher.group(i));
                }
            } catch (Exception e) {
                log.error("barcode rule parse error,regex={},parameter={}size={}", parseRule.getRegularExpression(), unionBarcode, parseRule.getResultFields().size(), e);
            }
        }
        return result;
    }

    private List<BarcodeParseResult> buildResult(List<String> compileResult, BarcodeParseRuleDTO parseRule) {
        if (CollectionUtils.isEmpty(compileResult)) {
            return Collections.emptyList();
        }
        if (compileResult.size() != parseRule.getResultFields().size()) {
            return Collections.emptyList();
        }

        List<BarcodeParseResult> barcodeParseResults = new ArrayList<>(parseRule.getResultFields().size());
        for (int i = 0; i < parseRule.getResultFields().size(); i++) {
            String field = parseRule.getResultFields().get(i);
            String result = compileResult.get(i);
            barcodeParseResults.add(BarcodeParseResult.builder().fieldName(field).fieldValue(result).build());
        }
        return barcodeParseResults;
    }
}
