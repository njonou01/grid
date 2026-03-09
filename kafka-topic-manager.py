#!/usr/bin/env python3

import logging
import sys
import argparse
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
log = logging.getLogger(__name__)

logging.getLogger('kafka.client_async').setLevel(logging.WARNING)
logging.getLogger('kafka.conn').setLevel(logging.WARNING)
logging.getLogger('kafka.protocol').setLevel(logging.WARNING)

BOOTSTRAP_SERVERS = [
    "192.168.1.26:30092",
    "192.168.1.26:30094",
    "192.168.1.26:30096"
]

def create_topic(topic_name, num_partitions=3, replication_factor=1):
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id='topic-manager')
    
    try:
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        
        fs = admin_client.create_topics(new_topics=[topic], validate_only=False)
        
        for name, future in fs.items():
            try:
                future.result()
                log.info(f"Topic '{name}' created successfully")
                log.info(f"  Partitions: {num_partitions}")
                log.info(f"  Replication Factor: {replication_factor}")
            except TopicAlreadyExistsError:
                log.error(f"Topic '{name}' already exists")
                sys.exit(1)
            except Exception as e:
                log.error(f"Failed to create topic '{name}': {e}")
                sys.exit(1)
    finally:
        admin_client.close()

def list_topics():
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id='topic-manager')
    
    try:
        metadata = admin_client.describe_topics()
        
        if not metadata:
            log.info("No topics found")
            return
        
        for topic_info in metadata:
            topic_name = topic_info['topic']
            partitions = topic_info['partitions']
            num_partitions = len(partitions)
            replication_factor = len(partitions[0]['replicas']) if partitions else 0
            
            log.info(f"Topic: {topic_name}, Partitions: {num_partitions}, Replication: {replication_factor}")
            
            for partition in partitions:
                partition_id = partition['partition']
                leader = partition['leader']
                replicas = ",".join(map(str, partition['replicas']))
                log.info(f"  Partition: {partition_id}, Leader: {leader}, Replicas: {replicas}")
        
    except Exception as e:
        log.error(f"Error listing topics: {e}")
        sys.exit(1)
    finally:
        admin_client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka topic manager')
    subparsers = parser.add_subparsers(dest='action', help='Action to perform')
    
    create_parser = subparsers.add_parser('create', help='Create a topic')
    create_parser.add_argument('--topic', required=True, help='Topic name')
    create_parser.add_argument('--partitions', type=int, default=3, help='Number of partitions (default: 3)')
    create_parser.add_argument('--replication', type=int, default=1, help='Replication factor (default: 1)')
    
    list_parser = subparsers.add_parser('list', help='List topics')
    
    args = parser.parse_args()
    
    if args.action == 'create':
        log.info(f"Creating topic '{args.topic}' with {args.partitions} partitions and replication factor {args.replication}...")
        create_topic(args.topic, args.partitions, args.replication)
    elif args.action == 'list':
        list_topics()
    else:
        parser.print_help()
        sys.exit(1)


