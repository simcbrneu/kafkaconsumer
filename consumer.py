#!/usr/bin/env python
#
# https://docs.confluent.io/current/clients/confluent-kafka-python/
#

import os
import tempfile

from confluent_kafka import Consumer, KafkaError

def get_keytab(url):
    try:
        # python 3
        from urllib.request import urlopen
    except ImportError:
        # python 2
        from urllib2 import urlopen

    fd, keytab_file = tempfile.mkstemp()
    r = urlopen(url)
    with os.fdopen(fd, "w+b") as f:
        while True:
            chunk = r.read(4096)
            if not chunk:
                break
            f.write(chunk)
    return keytab_file

def consumer():
    env_list = [
        'kafka_principal',
        'kafka_keytab',
        'kafka_brokers',
        'kafka_group',
        'kafka_topic',
    ]

    for env in env_list:
        envv = os.environ.get(env)
        if envv:
            globals()[env] = envv
        else:
            raise Exception("missing env '{}'".format(env))

    c = Consumer({
        'security.protocol': 'sasl_plaintext',
        'sasl.kerberos.principal': kafka_principal,
        'sasl.kerberos.keytab': get_keytab(kafka_keytab),
        'bootstrap.servers': kafka_brokers,
        'group.id': kafka_group,
        'auto.offset.reset': 'beginning',
    })
    c.subscribe([kafka_topic])
    while True:
        msg = c.poll(timeout=2)
        if not msg.error():
            print('Received message: %s' % msg.value().decode('utf-8'))
        elif msg.error().code() != KafkaError._PARTITION_EOF:
            print(msg.error())
            break
    c.close()

if __name__ == "__main__":
      consumer()
