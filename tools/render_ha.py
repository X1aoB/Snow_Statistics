"""Independent three-node Kafka KRaft / ZooKeeper exercise Compose file."""
import json
from pathlib import Path

services, volumes = {}, {}
for number in range(1, 4):
    name = f"broker{number}"
    volumes[name] = {}
    services[name] = {
        "image": "${KAFKA_IMAGE:?}", "profiles": ["kafka-ha"], "hostname": name,
        "mem_limit": "768m", "cpus": 1,
        "ports": [f"${{HA_IP:?}}:{19091 + number}:19092"],
        "environment": {
            "KAFKA_NODE_ID": number, "KAFKA_PROCESS_ROLES": "broker,controller",
            "KAFKA_LISTENERS": "INTERNAL://:9092,EXTERNAL://:19092,CONTROLLER://:9093",
            "KAFKA_ADVERTISED_LISTENERS": f"INTERNAL://{name}:9092,EXTERNAL://${{HA_IP:?}}:{19091 + number}",
            "KAFKA_INTER_BROKER_LISTENER_NAME": "INTERNAL",
            "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT",
            "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@broker1:9093,2@broker2:9093,3@broker3:9093",
            "KAFKA_DEFAULT_REPLICATION_FACTOR": 3, "KAFKA_MIN_INSYNC_REPLICAS": 2,
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": 3,
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": 3,
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": 2,
            "KAFKA_HEAP_OPTS": "-Xms256m -Xmx384m", "KAFKA_LOG_DIRS": "/var/lib/kafka/data",
            "KAFKA_LOG_RETENTION_BYTES": 134217728, "CLUSTER_ID": "V2U3OEVBNTcwNTJENDM2Qk"},
        "volumes": [f"{name}:/var/lib/kafka/data"],
        "logging": {"driver": "local", "options": {"max-size": "5m", "max-file": "2"}}}
    name = f"zk{number}"
    volumes[name] = {}
    volumes[name + "_log"] = {}
    services[name] = {
        "image": "${ZOOKEEPER_IMAGE:?}", "profiles": ["zookeeper"], "hostname": name,
        "mem_limit": "512m", "cpus": .5,
        "ports": [f"${{HA_IP:?}}:{12180 + number}:2181"],
        "environment": {"ZOO_MY_ID": number, "ZOO_SERVERS": "server.1=zk1:2888:3888;2181 server.2=zk2:2888:3888;2181 server.3=zk3:2888:3888;2181",
                        "JVMFLAGS": "-Xms128m -Xmx256m", "ZOO_4LW_COMMANDS_WHITELIST": "ruok,srvr,mntr",
                        "ZOO_STANDALONE_ENABLED": "false", "ZOO_ADMINSERVER_ENABLED": "false"},
        "volumes": [f"{name}:/data", f"{name}_log:/datalog"],
        "logging": {"driver": "local", "options": {"max-size": "5m", "max-file": "2"}}}
target = Path("lab/compose.ha.json")
target.write_text(json.dumps({"name": "snow-lab-ha", "services": services, "volumes": volumes}, indent=2) + "\n", newline="\n")
print(target)
