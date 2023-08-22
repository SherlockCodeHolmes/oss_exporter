#!/usr/local/python3.11/bin/python3

import time
from prometheus_client import REGISTRY, start_http_server
from oss_collector import OssCollector


def main():
    REGISTRY.register(OssCollector())
    start_http_server(8002)
    while True:
        time.sleep(60)


if __name__ == '__main__':
    main()
