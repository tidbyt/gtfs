#!/bin/bash

set -e

# I hate protobuf

protoc \
    --go_out=../proto \
    --go_opt=Mgtfs-realtime.proto=../proto \
    --go_opt=Mgtfs-realtime-NYCT.proto=../proto \
    gtfs-realtime.proto gtfs-realtime-NYCT.proto
