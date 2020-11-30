#!/bin/bash

logs_folder=$1;

for f in $(find "$logs_folder" -name '*.gz') ; do
  lines=$(gunzip -c "$f" | wc -l);
  echo "$f $lines";
done
