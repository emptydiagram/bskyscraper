-- Liquibase formatted SQL file

-- changeset nick.hamann:001
CREATE TABLE follow_record (
    user_did VARCHAR(255) NOT NULL,
    rkey VARCHAR(255) NOT NULL,
    subject_did VARCHAR(255) NOT NULL,
    PRIMARY KEY (user_did, rkey)
);
CREATE TABLE status (
    id INT PRIMARY KEY CHECK (id = 1),
    num_processed INT NOT NULL,
    cursor VARCHAR(255)
);
INSERT INTO status (id, num_processed) values (1, 0);