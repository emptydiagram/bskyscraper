package bskyscraper.bskyscraper.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import bskyscraper.bskyscraper.dto.FollowEvent;
import bskyscraper.bskyscraper.persistence.FollowRecord;
import bskyscraper.bskyscraper.persistence.FollowRecordId;
import bskyscraper.bskyscraper.repository.FollowRecordRepository;

@Service
public class FollowRecordService {
    private static Logger logger = LoggerFactory.getLogger(FollowRecordService.class);

    @Autowired
    private FollowRecordRepository followRecordRepo;

    public void saveEvent(FollowEvent event) throws Exception {
        if (event == null || event.getCommit() == null) {
            throw new IllegalArgumentException("Invalid follow event");
        }

        var commit = event.getCommit();
        var op = commit.getOperation().toLowerCase();
        switch (op) {
            case "create":
                var commitRecord = commit.getRecord();
                if (commitRecord == null) {
                    throw new IllegalArgumentException("Follow create event is missing record");
                }

                FollowRecordId id = new FollowRecordId(event.getDid(), commit.getRkey());
                var result = followRecordRepo.findById(id);

                if (result.isPresent()) {
                    if(commitRecord.getSubject().equals(result.get().getSubjectDid())) {
                        logger.warn("Record already exists for ({}, {}, {}), skipping.", event.getDid(), commit.getRkey(), result.get().getSubjectDid());
                    } else {
                        throw new Exception(String.format("Record for (%s, %s) already exists, but doesn't match given subject '%s'",
                            event.getDid(), commit.getRkey(), result.get().getSubjectDid()));
                    }
                }

                var record = new FollowRecord();
                record.setUserDid(event.getDid());
                record.setRkey(commit.getRkey());
                record.setSubjectDid(commitRecord.getSubject());
                followRecordRepo.save(record);

                break;
            case "delete":
                FollowRecordId frId = new FollowRecordId(event.getDid(), commit.getRkey());
                followRecordRepo.deleteById(frId);
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unsupported operation: '%s'", op));

        }
    }
}
