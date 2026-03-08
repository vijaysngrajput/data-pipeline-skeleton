

1. created yaml file

2. docker compose up -d

3. docker ps 

4. docker exec -it kafka bash
    - kafka-topics --list --bootstrap-server localhost:9092
    - kafka-topics --create \
        --topic test-topic \
        --bootstrap-server localhost:9092 \
        --partitions 1 \
        --replication-factor 1

    - kafka-topics --list --bootstrap-server localhost:9092
    - kafka-console-producer \
        --topic test-topic \
        --bootstrap-server localhost:9092
    >hello kafka             
    >

    >> ANOTHER TERMINAL
    - docker exec -it kafka bash
    - - kafka-console-consumer \
    --topic test-topic \
    --bootstrap-server localhost:9092 \
    --from-beginning





VS Code
   ↓
Dev Container (Linux)
   ↓
Python + PyFlink code
   ↓
Kafka (Docker)
   ↓
Flink (Docker)

apt update
apt install -y python3 python3-pip
ln -s /usr/bin/python3 /usr/bin/python
python --version
apt install -y default-jdk python3 python3-pip
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which javac))))
echo $JAVA_HOME
ls $JAVA_HOME/include
