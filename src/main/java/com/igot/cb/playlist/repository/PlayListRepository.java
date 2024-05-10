package com.igot.cb.playlist.repository;

import com.igot.cb.playlist.entity.PlayListEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PlayListRepository extends JpaRepository<PlayListEntity, String> {

}
