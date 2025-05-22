# Distributed System Challenges

1. Echo

```shell
 ./maelstrom test -w echo --bin ../../distributed_system_challenges/target/debug/echo \
     --node-count 1 \
     --time-limit 10
```

2. Unique ID

```shell
 ./maelstrom test -w unique-ids --bin ../../distributed_system_challenges/target/debug/unique_id \
     --node-count 3 \
     --time-limit 30 \
     --rate 1000 \
     --nemesis partition \
     --availability total
```

3. Broadcast

```shell
./maelstrom test -w broadcast --bin ../../distributed_system_challenges/target/debug/broadcast \
    --node-count 5 \
    --time-limit 20 \
    --rate 10
```

4. Grow-only Counter

```shell
./maelstrom test -w g-counter --bin ../../distributed_system_challenges/target/debug/grow_only_counter \
    --node-count 3 \
    --rate 100 \
    --time-limit 20 \
    --nemesis partition
```

5. Kafka-style Log

```shell
./maelstrom test -w kafka --bin ../../distributed_system_challenges/target/debug/kafka_style_log \
    --node-count 5 \
    --concurrency 2n \
    --time-limit 20 \
    --rate 1000
```

6. Totally available transactions

Read uncommitted

```shell
./maelstrom test -w txn-rw-register --bin ../../distributed_system_challenges/target/debug/totally_available_transactions \
    --node-count 5 \
    --time-limit 20 \
    --rate 1000 \
    --concurrency 2n \
    --consistency-models read-uncommitted \
    --availability total \
    --nemesis partition
```

Read committed
```shell
./maelstrom test -w txn-rw-register --bin ../../distributed_system_challenges/target/debug/totally_available_transactions \
    --node-count 5 \
    --time-limit 20 \
    --rate 1000 \
    --concurrency 2n \
    --consistency-models read-committed \
    --availability total \
    --nemesis partition
```

7. Raft

Step 1: Key-Value Store transfition
```shell
./maelstrom test -w lin-kv --bin ../../distributed_system_challenges/target/debug/raft \
    --time-limit 10 \
    --rate 10 \
    --node-count 1 \
    --concurrency 2n
```
