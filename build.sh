#!/usr/bin/env bash

cd $(dirname $0)

set -ex

for cmd in aindex
do
    go build ./cmd/$cmd/$cmd.go
done
