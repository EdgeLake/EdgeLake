#!/bin/bash

# copy anylog-network/anylog_node into EdgeLake
cp -r ~/AnyLog-Network/anylog_node/* edge_lake/
mv edge_lake/anylog.py edge_lake/edgelake.py
for file_path in edge_lake/*/*.py edge_lake/edge_lake.py ; do
  sed -i '' 's/AnyLog-Network/EdgeLake/g' ${file_path}
  sed -i '' "s/AL >/EL >/g" ${file_path}
  sed -i '' 's/anylog_node/edge_lake/g' ${file_path}
  sed -i '' 's/anylog_path/edgelake_path/g' ${file_path}
  sed -i '' 's/ANYLOG_LIB/EDGELAKE_LIB/g' ${file_path}
  sed -i '' 's/ANYLOG_HOME/EDGELAKE_HOME/g' ${file_path}
done
