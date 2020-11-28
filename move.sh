#!/bin/bash

path=Netease_news
files=$(ls $path)
for filename in $files; do
    if [ "${filename##*.}" = "json" ]; then
        cp $path/$filename data/$filename
        hdfs dfs -put $path/filename /json
    fi
    if [ "${filename##*.}" = "txt" ]; then
        hdfs dfs -put $path/filename /data
        mv $path/$filename data/$filename
    fi
done
