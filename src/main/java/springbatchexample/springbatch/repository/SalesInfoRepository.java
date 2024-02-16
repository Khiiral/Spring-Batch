package springbatchexample.springbatch.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import springbatchexample.springbatch.domain.SalesInfo;

@Repository
public interface SalesInfoRepository extends JpaRepository<SalesInfo, Long> {
    
}
