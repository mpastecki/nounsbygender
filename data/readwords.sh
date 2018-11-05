#!/bin/bash
while IFS='' read -r line || [[ -n "$line" ]]; do
  for word in $line; do
    grep "\<$word\>" /home/mpastecki/Downloads/PoliMorf-0.6.7.tab
  done;
done < "$1"
