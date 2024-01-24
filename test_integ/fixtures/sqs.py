import boto3
import pytest
import random

@pytest.fixture()
def sqs_extended_client(session):
    return session.client("sqs")

@pytest.fixture()
def queue_name():
    return f"IntegrationTestQueue{random.randint(0,10000)}"

@pytest.fixture()
def fifo_queue_name():
    return f"IntegrationTestQueue{random.randint(0,10000)}.fifo"

@pytest.fixture()
def queue(sqs_extended_client, queue_name):
    queue_object =  sqs_extended_client.create_queue(QueueName=queue_name)

    yield queue_object

    sqs_extended_client.purge_queue(
        QueueUrl=queue_object['QueueUrl']
    )

    sqs_extended_client.delete_queue(
        QueueUrl=queue_object['QueueUrl']
    )    


@pytest.fixture()
def fifo_queue(sqs_extended_client, fifo_queue_name):
    fifo_queue = sqs_extended_client.create_queue(QueueName=fifo_queue_name, Attributes={'FifoQueue': 'true'})

    yield fifo_queue

    sqs_extended_client.purge_queue(
        QueueUrl=fifo_queue['QueueUrl']
    )

    sqs_extended_client.delete_queue(
        QueueUrl=fifo_queue['QueueUrl']
    )


@pytest.fixture()
def sqs_extended_client_with_s3(sqs_extended_client):

    client_object = sqs_extended_client
    client_object.large_payload_support = f'integration-test-bucket-{random.randint(0, 10000)}'

    client_object.s3_client.create_bucket(
        Bucket=client_object.large_payload_support,
    )

    yield client_object


    client_object.s3_client.delete_bucket(
        Bucket=client_object.large_payload_support,
    )
