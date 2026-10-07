#!/bin/sh
# Adds a key and reads it back, on each node in turn, forever: enough traffic
# to watch it move on the dashboard. kurwadb's own bench/ is for measuring.
auth="authorization: Bearer $KURWA_AUTH_TOKEN"
i=0
while true; do
  for node in kurwadb kurwadb2 kurwadb3; do
    i=$((i + 1))
    curl -s -o /dev/null -X PUT -H "$auth" "http://$node:4040/sets/demo/k/key-$((i % 5000))"
    curl -s -o /dev/null -H "$auth" "http://$node:4040/sets/demo/k/key-$(((i * 7) % 5000))"
  done
done
