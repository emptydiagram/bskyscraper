package bskyscraper.bskyscraper.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import bskyscraper.bskyscraper.persistence.FollowRecord;
import bskyscraper.bskyscraper.persistence.FollowRecordId;

public interface FollowRecordRepository extends JpaRepository<FollowRecord, FollowRecordId> {}