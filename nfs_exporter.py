#!/usr/bin/python3

import sys
import subprocess

from collections import defaultdict
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse
from prometheus_client import (
    CollectorRegistry, Gauge, generate_latest, CONTENT_TYPE_LATEST
)

class DynamicMetricsHandler(BaseHTTPRequestHandler):
    """HTTP handler that gives metrics from ``core.REGISTRY``."""
    def do_GET(self):
        params = parse_qs(urlparse(self.path).query)
        registry = self.generator(params)
        if 'name[]' in params:
            registry = registry.restricted_registry(params['name[]'])
        try:
            output = generate_latest(registry)
        except:
            self.send_error(500, 'error generating metric output')
            raise
        self.send_response(200)
        self.send_header('Content-Type', CONTENT_TYPE_LATEST)
        self.end_headers()
        self.wfile.write(output)

    @staticmethod
    def factory(registry_generator):
        DynMetricsHandler = type('MetricsHandler',
                                (DynamicMetricsHandler, object),
                                {"generator": registry_generator})
        return DynMetricsHandler

def handler(metrics_handler, params):
    targets = params.get('target', None)
    if targets is None or len(targets) < 1:
        raise RuntimeError(f'invalid or missing target: {targets}')

    registry = CollectorRegistry()

    timer = Gauge('probe_duration_seconds', 'Duration of NFS probing',
                  registry=registry)

    success = True
    with timer.time():
        for target in targets:
            host, path = target.split(':')
            target = f'nfs://{host}{path}?version=4'
            result = subprocess.run(["nfs-ls", target], stdout=subprocess.PIPE)

            if result.returncode != 0:
                success = False
                break

    success_gauge = Gauge('probe_success', 'Success of NFS probing',
                          registry=registry)

    success_gauge.set(success)
    return registry

def main(addr):
    server = HTTPServer(addr, DynamicMetricsHandler.factory(handler))
    server.serve_forever()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} [address]", file=sys.stderr)
        exit(1)

    host, port = sys.argv[1].split(":")
    main((host, int(port)))
