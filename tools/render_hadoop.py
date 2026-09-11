import argparse
import xml.etree.ElementTree as ET
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--control", required=True)
parser.add_argument("--output", type=Path, default=Path("lab/generated/hadoop"))
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=True)
files = {
    "core-site.xml": {"fs.defaultFS": f"hdfs://{args.control}:9000", "hadoop.tmp.dir": "/tmp/hadoop-snow"},
    "hdfs-site.xml": {"dfs.replication": "2", "dfs.namenode.name.dir": "file:///data/name", "dfs.datanode.data.dir": "file:///data/dn", "dfs.namenode.rpc-address": f"{args.control}:9000"},
    "yarn-site.xml": {"yarn.resourcemanager.hostname": args.control, "yarn.nodemanager.resource.memory-mb": "3072", "yarn.scheduler.maximum-allocation-mb": "3072", "yarn.nodemanager.resource.cpu-vcores": "2", "yarn.nodemanager.aux-services": "mapreduce_shuffle", "yarn.nodemanager.local-dirs": "/data/yarn"},
    "hive-site.xml": {"hive.metastore.uris": f"thrift://{args.control}:9083", "hive.metastore.warehouse.dir": "/snow/warehouse", "javax.jdo.option.ConnectionURL": "jdbc:derby:;databaseName=/opt/hive/data/metastore_db;create=true"},
}
for name, properties in files.items():
    root = ET.Element("configuration")
    for key, value in properties.items():
        prop = ET.SubElement(root, "property")
        ET.SubElement(prop, "name").text = key
        ET.SubElement(prop, "value").text = value
    ET.ElementTree(root).write(args.output / name, encoding="utf-8", xml_declaration=True)
