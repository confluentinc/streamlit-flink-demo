from .log import get_logger
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import functools


log = get_logger()


# Mapping from message key to the configuration field we can use
# as a fallback if the message doesn't contain the expected key
fallbacks = {
  'environment': 'sql.current-catalog',
  'cluster': 'sql.current-database'
}


# confluent_kafka provides AvroProducer/Consumer implementations, but
# they are planned for removal in a future release, so we're using our own.
#
# This AvroProducer is also explicitly designed to be very easy to
# use for producing into multiple Kafka clusters, which the standard
# producers don't support.
class AvroProducer(object):

  def __init__(self, conf):
    # TODO(derekjn): generalize this class to support all serialization formats
    self.conf = conf
    self._producers = {}

  @functools.cache
  def get_sr_client(self, environment):
    section = self.conf[environment]
    return SchemaRegistryClient({
      'url': section['schema_registry']['endpoint'],
      'basic.auth.user.info': '%s:%s' % (
        section['schema_registry']['key'],
        section['schema_registry']['secret']
      )
    })

  @functools.cache
  def get_producer(self, environment, cluster):
    section = self.conf[environment][cluster]
    producer = Producer({
      'sasl.mechanisms': 'PLAIN',
      'security.protocol': 'SASL_SSL',
      'bootstrap.servers': section['endpoint'],
      'sasl.username': section['key'],
      'sasl.password': section['secret']
    })
    self._producers[environment, cluster] = producer
    return self._producers[environment, cluster]

  @functools.cache
  def get_serializer(self, environment, topic):
    sr = self.get_sr_client(environment)
    subject = '%s-value' % topic
    s = sr.get_latest_version(subject)
    return AvroSerializer(sr, s.schema.schema_str)

  def flush(self):
    for (env, cluster), producer in self._producers.items():
      producer.flush()
      log.debug('flushed %s.%s producer', env, cluster)

  def preflight(self, message):
    for f in ('topic', 'row'):
      if f not in message:
        raise ValueError("required field '%s' missing in message: %s" % (f, message))
    if 'key' in message:
      if message['key'] not in message['row']:
        raise ValueError("key field '%s' missing in row: %s" % (message['key'], message))
    # If we didn't get environment and cluster in the message, try
    # to resolve them from our configuration
    for k, fallback in fallbacks.items():
      if k in message:
        # Message already contains the expected key, on to the next one
        continue
      v = self.conf['flink'].get(fallback)
      if not v:
        raise ValueError('could not resolve %s for message: %s' % (k, message))
      message[k] = v

    # Now verify that whichever environment/cluster we ended up with actually
    # refer to environments/clusters in our configuration
    environment = message['environment']
    if environment not in self.conf:
      raise ValueError('environment %s not found in configuration: %s' % (environment, message))
    if message['cluster'] not in self.conf[environment]:
      raise ValueError('cluster %s not found in configuration: %s' % (message['cluster'], message))

  def produce(self, messages):
    if not isinstance(messages, list):
      messages = [messages]
    try:
      for m in messages:
        self.preflight(m)
        # If we made it through preflight, we are guaranteed to have an environment
        # and a cluster that our configuration file recognizes
        env = m['environment']
        cluster = m['cluster']
        topic = m['topic']
        row = m['row']
        # TODO(derekjn): handle non-string keys
        key = str(row[m['key']]) if 'key' in m else None
        producer = self.get_producer(env, cluster)
        serializer = self.get_serializer(env, topic)
        producer.produce(topic=topic, key=key,
          value=serializer(row, SerializationContext(topic, MessageField.VALUE)))
        log.debug('produced to %s.%s: %s', env, cluster, row)
      self.flush()
    except Exception as e:
      log.exception(e)