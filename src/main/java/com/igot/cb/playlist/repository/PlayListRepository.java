package com.igot.cb.playlist.repository;

import com.igot.cb.playlist.entity.PlayListEntity;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface PlayListRepository extends JpaRepository<PlayListEntity, String> {

  PlayListEntity findByOrgId(String orgId);

  //  @Query("SELECT * FROM playlist WHERE org_id = ?0 AND request_type = ?1")
  List<PlayListEntity> findByOrgIdAndRequestType(String orgId, String requestType);
}
