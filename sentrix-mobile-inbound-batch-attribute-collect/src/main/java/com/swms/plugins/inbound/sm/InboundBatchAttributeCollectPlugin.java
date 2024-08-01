package com.swms.plugins.inbound.sm;

import com.swms.common.utils.utils.DateFormatUtil;
import com.swms.plugin.extend.wms.inbound.IBatchAttributePlugin;
import com.swms.wms.api.inbound.dto.CustomerBatchAttributeParam;
import com.swms.wms.api.inbound.dto.CustomerBatchAttributeResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.pf4j.Extension;

import java.util.Map;

@Slf4j
@Extension
public class InboundBatchAttributeCollectPlugin implements IBatchAttributePlugin<CustomerBatchAttributeParam, CustomerBatchAttributeResult> {

    private static final String INBOUND_DATE = "inboundDate";
    private static final String CONTAINER_FACE = "containerFace";


    @Override
    public CustomerBatchAttributeResult doOperation(CustomerBatchAttributeParam customerBatchAttributeParam) {

        CustomerBatchAttributeResult customerBatchAttributeResult = new CustomerBatchAttributeResult();
        String containerFace = customerBatchAttributeParam.getContainerFace();
        if (StringUtils.isEmpty(containerFace)) {
            customerBatchAttributeResult.setBatchAttribute(customerBatchAttributeParam.getBatchAttributes());
            return customerBatchAttributeResult;
        }
        Map<String, Object> batchAttributes = Map.of(INBOUND_DATE, DateFormatUtil.getDateFormatYmdNowWithoutLink(), CONTAINER_FACE, containerFace);
        customerBatchAttributeResult.setBatchAttribute(batchAttributes);
        return customerBatchAttributeResult;
    }

}
