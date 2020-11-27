#!/bin/bash

path=Netease_news
files=$(ls $path)
for filename in $files; do
    if [ "${filename##*.}" = "json" ]; then
        cp $path/$filename data/$filename
    fi
done
