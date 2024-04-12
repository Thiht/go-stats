#!/usr/bin/env bash

cd "$(dirname "$0")/.." || exit 1

# Github-Ranking

date=$(date +%Y-%m-%d)

curl -Lo /tmp/github-ranking.csv "https://raw.githubusercontent.com/EvanLi/Github-Ranking/master/Data/github-ranking-$date.csv"

xsv index /tmp/github-ranking.csv

xsv search -s item Go /tmp/github-ranking.csv | \
  xsv select repo_url | \
  tail -n +2 | \
  head -n100 > data/top100.txt

head -n10 data/top100.txt > data/top10.txt

rm /tmp/github-ranking.csv /tmp/github-ranking.csv.idx


# awesome-go

curl -Lo /tmp/awesome-go.md "https://raw.githubusercontent.com/avelino/awesome-go/master/README.md"

awk '/## Artificial Intelligence/ { ok=1; }
/## Conferences/ { ok=0; }
ok && /^- / { print; }' /tmp/awesome-go.md | \
rg '^- \[[^]]+\]\(([^)]+)\).*$' -r '$1' | \
sed 's|/$||' > data/awesome-go.txt

rm /tmp/awesome-go.md

# seed

cat data/hand-picked.txt data/top100.txt data/awesome-go.txt | xargs -n1 | sort -u > data/seed.txt
