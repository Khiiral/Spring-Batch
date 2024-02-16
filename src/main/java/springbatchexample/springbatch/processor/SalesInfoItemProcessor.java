package springbatchexample.springbatch.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

import springbatchexample.springbatch.domain.SalesInfo;
import springbatchexample.springbatch.dto.SalesInfoDTO;
import springbatchexample.springbatch.service.SalesInfoService;


public class SalesInfoItemProcessor implements ItemProcessor<SalesInfoDTO, SalesInfo> {

   @Autowired
    private SalesInfoService salesInfoService;
    
    @Override
    public SalesInfo process(SalesInfoDTO item) throws Exception {
     
        return this.salesInfoService.mapToEntity(item);
    }
    
}
