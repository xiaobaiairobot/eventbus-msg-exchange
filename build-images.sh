#!/bin/bash

work_path=$(cd "$(dirname "$0")"; pwd)
version=1.0.0

usage() {
    echo """
    Usage: sh script.sh [all | demo ]
    """
    exit 1
}

# agent
function demo() {
  cd ${work_path}
  docker rmi -f eventbus-sdk-demo:${version}
  docker build -t eventbus-sdk-demo:${version} .
}

case "$1" in
  "all")
    demo
    ;;
  "demo")
    demo
    ;;
  *)
    usage
    ;;
esac