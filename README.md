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
     -- availability total
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
./maelstrom test -w g-counter --bin ../../distributed_system_challenges/target/debug/distributed_system_challenges \
    --node-count 3 
    --rate 100 
    --time-limit 20 
    --nemesis partition
```
