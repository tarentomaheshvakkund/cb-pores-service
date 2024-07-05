package com.igot.cb.playlist.repository;

import com.igot.cb.playlist.entity.PlayListEntity;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface PlayListRepository extends JpaRepository<PlayListEntity, String> {

  PlayListEntity findByOrgId(String orgId);

  List<PlayListEntity> findByOrgIdAndRequestTypeAndIsActive(String orgId, String requestType, Boolean isActive);

  PlayListEntity findByOrgIdAndIsActive(String orgId, Boolean isActive);

  PlayListEntity findByIdAndIsActive(String id, Boolean isActive);
}
