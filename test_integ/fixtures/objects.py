import boto3
import pytest
import random
import uuid
import copy

@pytest.fixture
def default_message_size_threshold():
    return 262144

@pytest.fixture
def small_message_body():
    return "s"


@pytest.fixture
def small_message_attribute(small_message_body):
    return {
        'Small_Message_Attribute': {
            'StringValue': small_message_body,
            'DataType': 'String'
        }
    }


@pytest.fixture
def large_message_body(small_message_body, default_message_size_threshold):
    return "s" * default_message_size_threshold

@pytest.fixture
def large_message_attribute(large_message_body):
    return  {
        'Large_Message_Attribute': {
            'StringValue': 'Test',
            'DataType': 'String'
        }
    }

@pytest.fixture()
def message_group_id():
    return 'test_group_id'

@pytest.fixture()
def message_deduplication_id():
    return str(uuid.uuid4())


@pytest.fixture()
def small_message_body_entries(small_message_body, small_message_attribute):
    return [
        {
            'Id': f"ID-{random.randint(0,10000)}",
            'MessageBody': small_message_body,
            'MessageAttributes': copy.deepcopy(small_message_attribute)
        },
        {
            'Id': f"ID-{random.randint(0,10000)}",
            'MessageBody': small_message_body,
            'MessageAttributes': copy.deepcopy(small_message_attribute)          
        }
    ]

@pytest.fixture()
def large_message_body_entries(large_message_body, small_message_attribute):
    # Going to be using small_message_attribute here since large_message_attribute
    # does not pertain to the size restrictions for message attributes

    return [
        {
            'Id': f"ID-{random.randint(0,10000)}",
            'MessageBody': large_message_body,
            'MessageAttributes': copy.deepcopy(small_message_attribute)
        },
        {
            'Id': f"ID-{random.randint(0,10000)}",
            'MessageBody': large_message_body,
            'MessageAttributes': copy.deepcopy(small_message_attribute)
        }
    ]

@pytest.fixture()
def small_message_body_fifo_entries(small_message_body_entries, message_group_id, message_deduplication_id):
    val1, val2 = message_deduplication_id + f"{random.randint(0, 10000)}", message_deduplication_id + f"{random.randint(0, 10000)}"

    for message in small_message_body_entries:
        message['MessageGroupId'] = message_group_id
        message['MessageDeduplicationId'] = message_deduplication_id + f"{random.randint(0, 10000)}"
    
    return small_message_body_entries

@pytest.fixture()
def large_message_body_fifo_entries(large_message_body_entries, message_group_id, message_deduplication_id):
    val1, val2 = message_deduplication_id + f"{random.randint(0, 10000)}", message_deduplication_id + f"{random.randint(0, 10000)}"

    for message in large_message_body_entries:
        message['MessageGroupId'] = message_group_id
        message['MessageDeduplicationId'] = message_deduplication_id + f"{random.randint(0, 10000)}"
    
    return large_message_body_entries