"""Deployable MQTT/clip contracts, rendered through the real Kustomize CLI."""
import subprocess
from pathlib import Path
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]

@pytest.fixture(scope='module')
def resources():
    result = subprocess.run(['kubectl', 'kustomize', str(ROOT / 'k8s/overlays/clips-local')],
                            text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    return {(r['kind'], r['metadata']['name']): r for r in yaml.safe_load_all(result.stdout) if r}

def test_gateways_have_distinct_certificate_identities_and_nonoverlapping_selectors(resources):
    seen = set()
    selectors = []
    for name in ('gateway', 'gateway-secondary'):
        d = resources['Deployment', name]
        assert d['spec']['replicas'] == 1
        assert d['spec']['strategy']['type'] == 'Recreate'
        pod = d['spec']['template']['spec']
        container = pod['containers'][0]
        env = {e['name']: e for e in container['env']}
        identity = env['CLIP_MQTT_CLIENT_ID']['value']
        assert identity not in seen
        seen.add(identity)
        assert env['MQTT_PKI']['value'] == '/mqtt-pki'
        secret = next(v['secret']['secretName'] for v in pod['volumes'] if v['name'] == 'mqtt-pki')
        assert secret == identity + '-pki'
        selectors.append(d['spec']['selector']['matchLabels'])
    assert selectors[0] != selectors[1]

def test_broker_persists_sessions_and_requires_certificate_topic_identity(resources):
    broker = resources['StatefulSet', 'mqtt']
    assert broker['spec']['replicas'] == 1
    assert broker['spec']['volumeClaimTemplates']
    config = next(v['data'] for (kind, name), v in resources.items()
                  if kind == 'ConfigMap' and name.startswith('mqtt-config-'))
    for line in ('require_certificate true', 'allow_anonymous false', 'use_username_as_clientid true'):
        assert line in config['mosquitto.conf']
    assert 'fleet/v1/%u/clip/request' in config['acl']
    assert resources['Service', 'mqtt']['spec']['ports'][0]['nodePort'] == 30883

def test_presigned_https_endpoint_and_proxy_preserve_signature(resources):
    config = resources['ConfigMap', 'fleet-config']['data']
    assert config['CLIP_MQTT_URI'] == 'ssl://mqtt:8883'
    assert config['OBJECT_STORE_PRESIGN_ENDPOINT'] == 'https://localhost:30443'
    assert resources['Service', 'minio-tls']['spec']['ports'][0]['nodePort'] == 30443
    proxy = next(v['data'] for (kind, name), v in resources.items()
                 if kind == 'ConfigMap' and name.startswith('minio-tls-config-'))
    assert 'proxy_set_header Host $http_host;' in proxy['default.conf']
    assert 'proxy_request_buffering off;' in proxy['default.conf']
    assert 'ssl_certificate_key' in proxy['default.conf']

def test_local_has_bridge_database_bootstrap_without_unrelated_flink_jobs(resources):
    assert ('Deployment', 'mqtt-bridge') in resources
    assert ('StatefulSet', 'postgres') in resources
    assert ('Job', 'minio-init') in resources
    assert ('Job', 'kafka-topics') in resources
    assert ('Job', 'flink-submit-pipelines') not in resources
    assert ('Job', 'clickhouse-schema') not in resources
    kafka = resources['StatefulSet', 'kafka']
    assert kafka['spec']['replicas'] == 1
    assert resources['Service', 'kafka']['spec']['publishNotReadyAddresses'] is True
    topics = resources['Job', 'kafka-topics']['spec']['template']['spec']['containers'][0]['command'][-1]
    assert '--replication-factor 1' in topics and 'min.insync.replicas=1' in topics
