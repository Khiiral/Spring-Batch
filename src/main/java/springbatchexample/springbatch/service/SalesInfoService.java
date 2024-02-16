package springbatchexample.springbatch.service;

import org.springframework.stereotype.Service;

import springbatchexample.springbatch.domain.SalesInfo;
import springbatchexample.springbatch.dto.SalesInfoDTO;

@Service
public class SalesInfoService {
    
    public SalesInfo mapToEntity(SalesInfoDTO salesInfoDTO) {
        SalesInfo entity = SalesInfo.builder()
                .product(salesInfoDTO.getProduct())
                .seller(salesInfoDTO.getSeller())
                .sellerId(salesInfoDTO.getSellerId())
                .price(salesInfoDTO.getPrice())
                .city(salesInfoDTO.getCity())
                .category(salesInfoDTO.getCategory())
                .build();
                
        return entity;
    }
}
