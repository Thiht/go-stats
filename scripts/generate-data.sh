#!/usr/bin/env bash

date=$(date +%Y-%m-%d)

curl -Lo /tmp/github-ranking.csv "https://raw.githubusercontent.com/EvanLi/Github-Ranking/master/Data/github-ranking-$date.csv"

xsv index /tmp/github-ranking.csv

xsv search -s item Go /tmp/github-ranking.csv | \
  xsv select repo_url | \
  tail -n +2 | \
  head -n100 | \
  sed 's/.*/&.git/' > data/top100.txt

head -n10 data/top100.txt > data/top10.txt

rm /tmp/github-ranking.csv /tmp/github-ranking.csv.idx
