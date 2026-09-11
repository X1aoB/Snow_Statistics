import argparse
import ipaddress
import xml.etree.ElementTree as ET
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--control", required=True)
parser.add_argument("--compute", required=True)
parser.add_argument("--output", type=Path, default=Path("lab/generated/hadoop"))
args = parser.parse_args()
args.control = str(ipaddress.ip_address(args.control))
args.compute = str(ipaddress.ip_address(args.compute))
args.output.mkdir(parents=True, exist_ok=True)
files = {
    "capacity-scheduler.xml": {
        "yarn.scheduler.capacity.root.queues": "default", "yarn.scheduler.capacity.root.default.capacity": "100",
        "yarn.scheduler.capacity.root.default.maximum-capacity": "100",
        "yarn.scheduler.capacity.maximum-am-resource-percent": "0.5",
    },
    "core-site.xml": {"fs.defaultFS": f"hdfs://{args.control}:9000", "hadoop.tmp.dir": "/tmp/hadoop-snow"},
    "hdfs-site.xml": {"dfs.replication": "2", "dfs.namenode.name.dir": "file:///data/name", "dfs.datanode.data.dir": "file:///data/dn", "dfs.namenode.rpc-address": f"{args.control}:9000", "dfs.client.use.datanode.hostname": "true", "dfs.datanode.use.datanode.hostname": "true"},
    "yarn-site.xml": {
        "yarn.resourcemanager.hostname": args.control, "yarn.nodemanager.hostname": args.compute,
        "yarn.nodemanager.resource.memory-mb": "2048", "yarn.scheduler.minimum-allocation-mb": "256",
        "yarn.scheduler.maximum-allocation-mb": "2048", "yarn.nodemanager.resource.cpu-vcores": "2",
        "yarn.nodemanager.aux-services": "mapreduce_shuffle", "yarn.nodemanager.local-dirs": "/data/yarn/local",
        "yarn.nodemanager.log-dirs": "/data/yarn/logs", "yarn.nodemanager.vmem-check-enabled": "false",
        "yarn.log-aggregation-enable": "true", "yarn.nodemanager.remote-app-log-dir": "/snow/yarn-logs",
        "yarn.log-aggregation.retain-seconds": "604800",
        "yarn.nodemanager.env-whitelist": "JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,PATH,LANG,TZ",
    },
    "hive-site.xml": {"hive.metastore.uris": f"thrift://{args.control}:9083", "hive.metastore.warehouse.dir": "/snow/warehouse", "javax.jdo.option.ConnectionURL": "jdbc:derby:;databaseName=/opt/hive/data/metastore_db;create=true"},
}
for name, properties in files.items():
    root = ET.Element("configuration")
    for key, value in properties.items():
        prop = ET.SubElement(root, "property")
        ET.SubElement(prop, "name").text = key
        ET.SubElement(prop, "value").text = value
    ET.ElementTree(root).write(args.output / name, encoding="utf-8", xml_declaration=True)
