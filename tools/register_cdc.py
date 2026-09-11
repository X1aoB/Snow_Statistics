import os

import httpx
import pymysql

host = os.environ["CONTROL_IP"]
secret = os.environ["LAB_CDC_PASSWORD"]
db = pymysql.connect(host=host, user="root", password=os.environ["LAB_MYSQL_ROOT_PASSWORD"])
with db:
    with db.cursor() as cursor:
        cursor.execute("CREATE USER IF NOT EXISTS 'snow_cdc'@%s IDENTIFIED BY %s", ("%", secret))
        cursor.execute("ALTER USER 'snow_cdc'@%s IDENTIFIED BY %s", ("%", secret))
        cursor.execute("GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'snow_cdc'@'%'")
    db.commit()
config = {"connector.class": "io.debezium.connector.mysql.MySqlConnector", "tasks.max": "1",
          "database.hostname": host, "database.port": "3306", "database.user": "snow_cdc", "database.password": secret,
          "database.server.id": "184054", "topic.prefix": "snow.synthetic.cdc", "database.include.list": "snow_ops",
          "table.include.list": "snow_ops.contents,snow_ops.campaigns,snow_ops.tickets",
          "include.schema.changes": "true", "provide.transaction.metadata": "true",
          "schema.history.internal.kafka.bootstrap.servers": host + ":9092",
          "schema.history.internal.kafka.topic": "snow.internal.schema-history", "snapshot.mode": "initial",
          "key.converter": "org.apache.kafka.connect.json.JsonConverter", "value.converter": "org.apache.kafka.connect.json.JsonConverter",
          "key.converter.schemas.enable": "false", "value.converter.schemas.enable": "false"}
response = httpx.put(f"http://{host}:8083/connectors/snow-synthetic-ops/config", json=config, timeout=30)
if response.is_error:
    raise SystemExit(f"Connector registration failed: HTTP {response.status_code}")
print("Registered snow-synthetic-ops (configuration omitted to protect credentials)")
