CREATE TABLE block_meta
(
    id          String, 
	PRIMARY KEY (id),
    at          String,
    number      Int32,
    hash        String,
    parent_hash String,
    timestamp   String
)
ENGINE = MergeTree()
ORDER BY id

CREATE TABLE IF NOT EXISTS cursors
(
    id         String,
    cursor     String,
    block_num  Int64,
    block_id   String,
    PRIMARY KEY (id)
) ENGINE = MergeTree()
ORDER BY id