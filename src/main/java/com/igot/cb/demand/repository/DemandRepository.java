package com.igot.cb.demand.repository;

import com.igot.cb.demand.entity.DemandEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DemandRepository extends JpaRepository<DemandEntity, String> {

}
