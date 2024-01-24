from sqs_extended_client import SQSExtendedClientSession
from .fixtures.session import *
from .fixtures.sqs import *
from .fixtures.objects import *
from . import logger
import copy
import logging



def send_message_helper(sqs_extended_client, queue_url, message_body, message_attributes, message_group_id = None, message_deduplication_id = None, **kwargs):
    """

    Acts as a helper for sending a message via the SQS Extended Client.

    sqs_extended_client: The SQS Extended Client
    queue_url: The URL associated with the SQS Queue
    message_body: The message body
    message_attributes: The message attributes

    """

    send_message_kwargs = {
        'QueueUrl' : queue_url,
        'MessageBody' : message_body,
        'MessageAttributes' : message_attributes.copy(),
    }

    if message_group_id:
        send_message_kwargs['MessageGroupId'] = message_group_id
        send_message_kwargs['MessageDeduplicationId'] = message_deduplication_id

    logger.info("Sending the message via the SQS Extended Client")

    response = sqs_extended_client.send_message(**send_message_kwargs)

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200

    return


def send_message_batch_helper(sqs_extended_client, queue_url, entries):
    """

    Acts as a helper for sending batches of message via the SQS Extended Client.

    sqs_extended_client: The SQS Extended Client
    queue_url: The URL associated with the SQS Queue
    entries: The batches of messages to be sent

    """

    send_message_batch_kwargs = {
        'QueueUrl' : queue_url,
        'Entries': copy.deepcopy(entries)
    }

    response = sqs_extended_client.send_message_batch(**send_message_batch_kwargs)

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(response['Successful']) == len(entries)

    return

def check_receive_message_non_batch(receive_message_response, message_body, message_attributes):
    """

    Responsible that the response returned after calling "receive_message" is valid
    for the associated message_body and message_attributes.

    receive_message_response: Response received after calling "receive_message"
    message_body: The message body 
    message_attributes: The message attributes

    """
    
    assert receive_message_response['ResponseMetadata']['HTTPStatusCode'] == 200
    messages_list = receive_message_response.get('Messages', [])

    assert len(messages_list) == 1

    message = messages_list[0]

    assert message['Body'] == message_body
    assert message['MessageAttributes'] == message_attributes

    return 

def check_receive_message_batch(receive_message_batch_response, entries):
    """

    Responsible that the response returned after calling "receive_message" is valid
    for the associated message batch entries.

    receive_message_response: Response received after calling "receive_message"
    entries: The batch entries

    """
    assert receive_message_batch_response['ResponseMetadata']['HTTPStatusCode'] == 200
    messages = receive_message_batch_response.get('Messages', [])

    assert len(messages) == len(entries)

    for pos, message in enumerate(messages):
        assert message['Body'] == entries[pos]['MessageBody']
        assert message['MessageAttributes'] == entries[pos]['MessageAttributes']

    return

def is_s3_bucket_empty(sqs_extended_client):
    """

    Responsible for checking if the S3 bucket created consists 
    of objects at the time of calling the function.

    sqs_extended_client: The SQS Extended Client
    
    """
    response = sqs_extended_client.s3_client.list_objects_v2(
        Bucket=sqs_extended_client.large_payload_support
    )

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    return "Contents" not in response

def change_message_visibility_helper(sqs_extended_client, queue_url, receive_message_response, visibility_timeout, **kwargs):
    """
    Responsible for changing the message visibility for a single message received from calling 
    the `receive_message` API. If the visibility_timeout to be set is below a minute, for 
    testing purposes, then the old receipt handle is returned. Otherwise, the function 
    returns the new receipt handle received after calling `receive_message` again.

    sqs_extended_client: The SQS Extended Client
    queue_url: The SQS Queue URL
    receive_message_response: The response received after calling `receive_message`
    visibility_timeout: The visibility timeout to set
    
    """
    receipt_handle = receive_message_response['Messages'][0]['ReceiptHandle']
    receipt_handle_to_return = receipt_handle

    visibility_kwargs = {
        'QueueUrl': queue_url,
        'ReceiptHandle': receipt_handle,
        'VisibilityTimeout': visibility_timeout
    }

    response = sqs_extended_client.change_message_visibility(**visibility_kwargs)

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    
    # Setting the check to be with 0 here since the other test has a placeholder value of 2400. 
    if visibility_timeout > 0:
        receive_message_response = receive_message_helper(sqs_extended_client=sqs_extended_client, queue_url=visibility_kwargs['QueueUrl'], **kwargs)
        assert 'Messages' not in receive_message_response

    else:
        receive_message_response = receive_message_helper(sqs_extended_client=sqs_extended_client, queue_url=visibility_kwargs['QueueUrl'], **kwargs)
        check_receive_message_non_batch(receive_message_response, kwargs["message_body"], kwargs['message_attributes'])
        receipt_handle_to_return = receive_message_response['Messages'][0]['ReceiptHandle']
    
    return receipt_handle_to_return


def change_message_visibility_batch_helper(sqs_extended_client, queue_url, receive_message_batch_response, visibility_timeout, message_entries, **kwargs):
    
    entries = []
    response_to_return = copy.deepcopy(receive_message_batch_response)

    for messages in receive_message_batch_response['Messages']:
        individual_entry = {}
        individual_entry['Id'] = messages['MessageId']
        individual_entry['ReceiptHandle'] = messages['ReceiptHandle']
        individual_entry['VisibilityTimeout'] = visibility_timeout
        entries.append(individual_entry.copy())
    
    visibility_batch_kwargs = {
        'QueueUrl': queue_url,
        'Entries': copy.deepcopy(entries)
    }

    response = sqs_extended_client.change_message_visibility_batch(**visibility_batch_kwargs)

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(response['Successful']) == len(message_entries)

    # Setting the check to be with 0 here since the other test has a placeholder value of 2400. 
    if visibility_timeout > 0:
        receive_message_batch_response = receive_message_helper(sqs_extended_client=sqs_extended_client, queue_url=visibility_batch_kwargs['QueueUrl'], entries=copy.deepcopy(message_entries), **kwargs)
        assert 'Messages' not in receive_message_batch_response
    else:
        receive_message_batch_response = receive_message_helper(sqs_extended_client=sqs_extended_client, queue_url=visibility_batch_kwargs['QueueUrl'], entries=copy.deepcopy(message_entries), **kwargs)
        check_receive_message_batch(receive_message_batch_response, message_entries)
        response_to_return = copy.deepcopy(receive_message_batch_response)

    return response_to_return



def receive_message_helper(sqs_extended_client, queue_url, through_fifo = False, long_polling = False, entries = None, **kwargs) -> dict:
    """

    Acts as a helper for receiving a message via the SQS Extended Client.

    sqs_extended_client: The SQS Extended Client
    queue_url: The URL associated with the SQS Queue
    through_fifo: Boolean indicating if the Queue created is a FIFO Queue
    long_polling: Boolean indicating if long polling is enabled
    entries: The batch entries consisting of message payloads

    Returns the message received from the Queue
    """

    # Extracting the message attributes in a payload, if present, rather than requesting 'All'.
    original_message_attributes = []

    if not entries:
        original_message_attributes = list(kwargs['message_attributes'].keys())
    else:
        for message_entry in entries:
            message_attributes = list(message_entry['MessageAttributes'].keys())
            original_message_attributes.extend(message_attributes)

    receive_message_kwargs = {
        'QueueUrl': queue_url, 
        'MessageAttributeNames': original_message_attributes.copy()
    }

    if through_fifo:
        receive_message_kwargs['AttributeNames'] = ["MessageGroupId", "MessageDeduplicationId"]
    
    if long_polling:
        receive_message_kwargs['MaxNumberOfMessages'] = 2
        receive_message_kwargs['WaitTimeSeconds'] = 10
    
    logger.info("Receiving the message via the SQS Extended Client")

    response = sqs_extended_client.receive_message(**receive_message_kwargs)

    return response


def delete_messages_helper(sqs_extended_client, queue_url, receipt_handle):
    """

    Acts as a helper for deleting messages via the SQS Extended Client.

    sqs_extended_client: The SQS Extended Client
    queue_url: The URL associated with the SQS Queue
    message: The message to be deleted

    """

    logger.info("Deleting the message via the SQS Extended Client")

    response = sqs_extended_client.delete_message(
        QueueUrl = queue_url,
        ReceiptHandle = receipt_handle
    )
    
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    return

def delete_message_batch_helper(sqs_extended_client, queue_url, entries):
    """

    Acts as a helper for deleting message batches via the SQS Extended Client.

    sqs_extended_client: The SQS Extended Client
    queue_url: The URL associated with the SQS Queue
    entries: The message entries to be deleted

    """
    response = sqs_extended_client.delete_message_batch(
        QueueUrl = queue_url,
        Entries = entries
    )

    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    assert len(response['Successful']) == len(entries)
    return




def initialize_extended_client_attributes_through_s3(sqs_extended_client):
    """

    Acts as a helper for adding attributes to the extended client
    which are required for sending in payloads to S3 buckets.

    sqs_extended_client: The SQS Extended Client

    """

    sqs_extended_client.always_through_s3 = True
    sqs_extended_client.use_legacy_attribute = False
    sqs_extended_client.delete_payload_from_s3 = True
    
    return


def perform_send_receive_delete_workflow(sqs_extended_client, queue, message_body, 
                                         message_attributes, message_group_id = None, message_deduplication_id = None, change_message_visibility = False, new_message_visibility = None):
    """
    Responsible for replicating a workflow of sending, receiving and deleting a message
    by calling the helper functions as and when necessary.

    sqs_extended_client: The SQS Extended Client
    queue: The SQS Queue
    message_body: The message body
    message_attributes: The message attributes
    message_group_id: Tag specifying a message belongs to a message group (Required for FIFO Queues)
    message_deduplication_id: Token used for deduplication of sent message (Required for FIFO Queues)

    """
    
    queue_url = queue["QueueUrl"]

    kwargs = {
        'sqs_extended_client': sqs_extended_client,
        'queue_url': queue_url,
        'message_body': message_body,
        'message_attributes': message_attributes
    }

    if message_group_id:
        kwargs['message_group_id'] = message_group_id
        kwargs['message_deduplication_id'] = message_deduplication_id

        kwargs['through_fifo'] = True

    send_message_helper(**kwargs)

    # Verifying that the S3 object was added
    if sqs_extended_client.large_payload_support:
        assert not is_s3_bucket_empty(sqs_extended_client)

    receive_message_response = receive_message_helper(**kwargs)

    old_receipt_handle = receive_message_response['Messages'][0]['ReceiptHandle']
    new_receipt_handle = old_receipt_handle

    check_receive_message_non_batch(receive_message_response, message_body, message_attributes)

    # Change the message visibility here when the parameter (change_message_visibility) is true
    if change_message_visibility:
        new_receipt_handle = change_message_visibility_helper(receive_message_response=receive_message_response, 
                                         visibility_timeout=new_message_visibility, **kwargs)    

    delete_messages_helper(sqs_extended_client, queue_url, new_receipt_handle)

    return


def perform_send_receive_delete_batch_workflow(sqs_extended_client, queue, entries, through_fifo = False, change_message_visibility = False, new_message_visibility = None):
    """
    Responsible for replicating a workflow of sending, receiving and deleting message batches
    by calling the helper functions as and when necessary.

    sqs_extended_client: The SQS Extended Client
    queue_url: The URL associated with the SQS Queue
    entries: The message entries
    through_fifo: Boolean indicating if the Queue created is a FIFO Queue

    """

    queue_url = queue["QueueUrl"]

    kwargs = {
        'sqs_extended_client': sqs_extended_client,
        'queue_url': queue_url,
        'MaxNumberOfMessages': 2,
        'WaitTimeSeconds': 10,
        'long_polling': True
    }

    if through_fifo:
        kwargs['through_fifo'] = through_fifo

    
    queue_url = queue["QueueUrl"]
    send_message_batch_helper(sqs_extended_client, queue_url, copy.deepcopy(entries))

    # Verifying that the S3 object was added
    if sqs_extended_client.large_payload_support:
        assert not is_s3_bucket_empty(sqs_extended_client)

    receive_message_response = receive_message_helper(entries=copy.deepcopy(entries), **kwargs)

    check_receive_message_batch(receive_message_response, copy.deepcopy(entries))

    # Change the message visibility here when the parameter (change_message_visibility_batch) is true
    if change_message_visibility:
        receive_message_response = change_message_visibility_batch_helper(receive_message_batch_response=receive_message_response, 
                                                                          visibility_timeout=new_message_visibility, message_entries=copy.deepcopy(entries), **kwargs)
              

    delete_message_entries = []

    for message in receive_message_response['Messages']:
        delete_message_entries.append(
            {
            "Id": message['MessageId'],
            "ReceiptHandle": message['ReceiptHandle']
            }
        )

    delete_message_batch_helper(sqs_extended_client, queue_url, copy.deepcopy(delete_message_entries))
    return

def test_session(session):
    assert boto3.session.Session == SQSExtendedClientSession

def test_send_receive_delete_small_message_not_through_s3(sqs_extended_client, queue, small_message_body, small_message_attribute):
    """
    Responsible for testing the entire workflow of sending, receiving and deleting
    a small message using the SQS Extended Client. This test shall send a small message
    payload with the attribute `always_through_s3` set to False.

    sqs_extended_client: The SQS Extended Client
    queue: The SQS Queue
    small_message_body: Message payload not exceeding the limit of 256 KB
    small_message_attribute: Message attributes for the payload

    """

    logger.info("Initiating execution of test_send_receive_delete_small_message_not_through_s3")

    sqs_extended_client.always_through_s3 = False
    perform_send_receive_delete_workflow(sqs_extended_client, queue, small_message_body, small_message_attribute)

    logger.info("Completed execution of test_send_receive_delete_small_message_not_through_s3")


    return


def test_send_receive_delete_small_message_through_s3(sqs_extended_client_with_s3, queue, small_message_body, small_message_attribute):
    """
    Responsible for testing the entire workflow of sending, receiving and deleting
    a small message using the SQS Extended Client. This test shall send a small message
    payload with the attribute `always_through_s3` set to True.

    sqs_extended_client_with_s3: The SQS Extended Client with a S3 bucket created
    queue: The SQS Queue
    small_message_body: Message payload not exceeding the limit of 256 KB
    small_message_attribute: Message attributes for the payload

    """
    
    logger.info("Initiating execution of test_send_receive_delete_small_message_through_s3")

    initialize_extended_client_attributes_through_s3(sqs_extended_client_with_s3)
    perform_send_receive_delete_workflow(sqs_extended_client_with_s3, queue, small_message_body, small_message_attribute)

    assert is_s3_bucket_empty(sqs_extended_client_with_s3)
    logger.info("Completed execution of test_send_receive_delete_small_message_through_s3")

    return


def test_send_receive_delete_large_message(sqs_extended_client_with_s3, queue, large_message_body, large_message_attribute):
    """
    Responsible for testing the entire workflow of sending, receiving and deleting
    a large message using the SQS Extended Client. This test shall send a large message
    payload which shall always create a S3 bucket to store the original contents of the payload.

    sqs_extended_client_with_s3: The SQS Extended Client with a S3 bucket created
    queue: The SQS Queue
    large_message_body: Message payload exceeding the limit of 256 KB
    large_message_attribute: Message attributes for the large payload

    """    
    
    logger.info("Initiating execution of test_send_receive_delete_large_message")


    initialize_extended_client_attributes_through_s3(sqs_extended_client_with_s3)
    perform_send_receive_delete_workflow(sqs_extended_client_with_s3, queue, large_message_body, large_message_attribute)

    assert is_s3_bucket_empty(sqs_extended_client_with_s3)
    logger.info("Completed execution of test_send_receive_delete_large_message")


    return


def test_send_receive_delete_small_message_not_through_s3_fifo_queue(sqs_extended_client, fifo_queue, small_message_body, small_message_attribute, message_group_id, message_deduplication_id):

    """
    Responsible for testing the entire workflow of sending, receiving and deleting
    a small message using the SQS Extended Client. This test shall send a small message
    payload with the attribute `always_through_s3` set to False to a FIFO queue.

    sqs_extended_client: The SQS Extended Client
    queue: The SQS Queue
    small_message_body: Message payload not exceeding the limit of 256 KB
    small_message_attribute: Message attributes for the payload
    message_group_id: Tag specifying the message belonging to a message group (required for FIFO queues)
    message_deduplication_id: Token used for deduplication of sent messages (required for FIFO queues)

    """

    logger.info("Initiating execution of test_send_receive_delete_small_message_not_through_s3_fifo_queue")


    sqs_extended_client.always_through_s3 = False
    perform_send_receive_delete_workflow(sqs_extended_client, fifo_queue, small_message_body, small_message_attribute, message_group_id, message_deduplication_id)
    
    logger.info("Completed execution of test_send_receive_delete_small_message_not_through_s3_fifo_queue")

    return


def test_send_receive_delete_small_message_through_s3_fifo_queue(sqs_extended_client_with_s3, fifo_queue, small_message_body, small_message_attribute, message_group_id, message_deduplication_id):

    """
    Responsible for testing the entire workflow of sending, receiving and deleting
    a small message using the SQS Extended Client. This test shall send a small message
    payload with the attribute `always_through_s3` set to True to a FIFO queue.

    sqs_extended_client_with_s3: The SQS Extended Client with a S3 bucket created
    queue: The SQS Queue
    small_message_body: Message payload not exceeding the limit of 256 KB
    small_message_attribute: Message attributes for the payload
    message_group_id: Tag specifying the message belonging to a message group (required for FIFO queues)
    message_deduplication_id: Token used for deduplication of sent messages (required for FIFO queues)

    """

    logger.info("Initiating execution of test_send_receive_delete_small_message_through_s3_fifo_queue")

    initialize_extended_client_attributes_through_s3(sqs_extended_client_with_s3)
    perform_send_receive_delete_workflow(sqs_extended_client_with_s3, fifo_queue, small_message_body, small_message_attribute, message_group_id, message_deduplication_id)

    assert is_s3_bucket_empty(sqs_extended_client_with_s3)
    logger.info("Completed execution of test_send_receive_delete_small_message_through_s3_fifo_queue")

    return
    
def test_send_receive_delete_large_message_fifo_queue(sqs_extended_client_with_s3, fifo_queue, large_message_body, large_message_attribute, message_group_id, message_deduplication_id):
    """
    Responsible for testing the entire workflow of sending, receiving and deleting
    a large message using the SQS Extended Client. This test shall send a large message
    payload which shall always create a S3 bucket to store the original contents of the payload.

    sqs_extended_client_with_s3: The SQS Extended Client with a S3 bucket created
    queue: The SQS Queue
    large_message_body: Message payload exceeding the limit of 256 KB
    large_message_attribute: Message attributes for the large payload
    message_group_id: Tag specifying the message belonging to a message group (required for FIFO queues)
    message_deduplication_id: Token used for deduplication of sent messages (required for FIFO queues

    """    

    logger.info("Initiating execution of test_send_receive_delete_large_message_fifo_queue")

    initialize_extended_client_attributes_through_s3(sqs_extended_client_with_s3)
    perform_send_receive_delete_workflow(sqs_extended_client_with_s3, fifo_queue, large_message_body, large_message_attribute, message_group_id, message_deduplication_id)

    assert is_s3_bucket_empty(sqs_extended_client_with_s3)

    logger.info("Completed execution of test_send_receive_delete_large_message_fifo_queue")


    return


def test_small_message_batch_not_through_s3(sqs_extended_client, queue, small_message_body_entries):
    """
    Responsible for testing the entire workflow of sending, receiving and deleting
    small messages sent as a batch using the SQS Extended Client.

    sqs_extended_client: The SQS Extended Client 
    queue: The SQS Queue
    small_message_body_entries: The batch entries consisting of small message bodies and small message attributes

    """  
    logger.info("Initiating execution of test_small_message_batch_not_through_s3")

    perform_send_receive_delete_batch_workflow(sqs_extended_client, queue, copy.deepcopy(small_message_body_entries))

    logger.info("Completed execution of test_small_message_batch_not_through_s3")

    return

def test_small_message_batch_through_s3(sqs_extended_client_with_s3, queue, small_message_body_entries):
    """
    Responsible for testing the entire workflow of sending, receiving and deleting
    small messages sent as a batch using the SQS Extended Client. This test shall send the batch
    payload which shall create a S3 bucket to store the original contents of the payload.

    sqs_extended_client_with_s3: The SQS Extended Client with a S3 bucket created
    queue: The SQS Queue
    small_message_body_entries: The batch entries consisting of small message bodies and small message attributes

    """  
    logger.info("Initiating execution of test_small_message_batch_through_s3")

    initialize_extended_client_attributes_through_s3(sqs_extended_client_with_s3)
    perform_send_receive_delete_batch_workflow(sqs_extended_client_with_s3, queue, copy.deepcopy(small_message_body_entries))
    assert is_s3_bucket_empty(sqs_extended_client_with_s3)

    logger.info("Completed execution of test_small_message_batch_through_s3")

    return


def test_large_message_batch(sqs_extended_client_with_s3, queue, large_message_body_entries):
    """
    Responsible for testing the entire workflow of sending, receiving and deleting
    large messages sent as a batch using the SQS Extended Client. This test shall send the batch
    payload which shall create a S3 bucket to store the original contents of the payload.

    sqs_extended_client_with_s3: The SQS Extended Client with a S3 bucket created
    queue: The SQS Queue
    large_message_body_entries: The batch entries consisting of large message bodies

    """  
    logger.info("Initiating execution of test_large_message_batch")

    initialize_extended_client_attributes_through_s3(sqs_extended_client_with_s3)
    perform_send_receive_delete_batch_workflow(sqs_extended_client_with_s3, queue, copy.deepcopy(large_message_body_entries))
    assert is_s3_bucket_empty(sqs_extended_client_with_s3)

    logger.info("Completed execution of test_large_message_batch")

    return

def test_small_message_batch_not_through_s3_fifo_queue(sqs_extended_client, fifo_queue, small_message_body_fifo_entries):
    """
    Responsible for testing the entire workflow of sending, receiving and deleting
    small messages sent as a batch using the SQS Extended Client by sending it to a FIFO queue.

    sqs_extended_client: The SQS Extended Client
    fifo_queue: The SQS FIFO based Queue
    small_message_body_fifo_entries: The batch entries consisting of small message bodies along with required attributes
    for FIFO queues

    """  
    logger.info("Initiating execution of test_small_message_batch_not_through_s3_fifo_queue")
    
    perform_send_receive_delete_batch_workflow(sqs_extended_client, fifo_queue, copy.deepcopy(small_message_body_fifo_entries), through_fifo=True)

    logger.info("Completed execution of test_small_message_batch_not_through_s3_fifo_queue")
    return


def test_small_message_batch_through_s3_fifo_queue(sqs_extended_client_with_s3, fifo_queue, small_message_body_fifo_entries):
    """
    Responsible for testing the entire workflow of sending, receiving and deleting
    small messages sent as a batch using the SQS Extended Client by sending it to a FIFO queue as well as through
    S3.

    sqs_extended_client_with_s3: The SQS Extended Client with a S3 bucket created
    fifo_queue: The SQS FIFO based Queue
    small_message_body_fifo_entries: The batch entries consisting of small message bodies along with required attributes
    for FIFO queues

    """
    logger.info("Initiating execution of test_small_message_batch_through_s3_fifo_queue")

    initialize_extended_client_attributes_through_s3(sqs_extended_client_with_s3)
    perform_send_receive_delete_batch_workflow(sqs_extended_client_with_s3, fifo_queue, copy.deepcopy(small_message_body_fifo_entries), through_fifo=True)
    assert is_s3_bucket_empty(sqs_extended_client_with_s3)

    logger.info("Completed execution of test_small_message_batch_through_s3_fifo_queue")

    return

def test_large_message_batch_fifo_queue(sqs_extended_client_with_s3, fifo_queue, large_message_body_fifo_entries):
    """
    Responsible for testing the entire workflow of sending, receiving and deleting
    large messages sent as a batch using the SQS Extended Client by sending it to a FIFO queue as well as through
    S3.

    sqs_extended_client_with_s3: The SQS Extended Client with a S3 bucket created
    fifo_queue: The SQS FIFO based Queue
    large_message_body_fifo_entries: The batch entries consisting of large message bodies along with the message attributes
    for FIFO queues

    """
    logger.info("Initiating execution of test_large_message_batch_fifo_queue")
    
    initialize_extended_client_attributes_through_s3(sqs_extended_client_with_s3)
    perform_send_receive_delete_batch_workflow(sqs_extended_client_with_s3, fifo_queue, copy.deepcopy(large_message_body_fifo_entries), through_fifo=True)
    assert is_s3_bucket_empty(sqs_extended_client_with_s3)
    
    logger.info("Completed execution of test_large_message_batch_fifo_queue")

    return


# Tests for change_message_visibility

def test_change_message_visibility_to_small_value(sqs_extended_client_with_s3, queue, large_message_body, large_message_attribute):
    """
    Responsible for testing the entire workflow of sending, receiving, changing the message visibility and deleting a message
    using the SQS Extended Client through S3. The message visibility shall be changed to a small value here for the purpose of being able
    to be received from the Queue again.

    sqs_extended_client_with_s3: The SQS Extended Client with a S3 bucket created
    queue: The SQS Queue
    large_message_body: Message payload exceeding the limit of 256 KB
    large_message_attribute: Message attributes for the payload
    
    """
    logger.info("Initiating execution of test_change_message_visibility_to_small_value")

    initialize_extended_client_attributes_through_s3(sqs_extended_client_with_s3)
    perform_send_receive_delete_workflow(sqs_extended_client_with_s3, queue, large_message_body, copy.deepcopy(large_message_attribute), change_message_visibility=True, new_message_visibility=0)
    assert is_s3_bucket_empty(sqs_extended_client_with_s3)

    logger.info("Completed execution of test_change_message_visibility_to_small_value")
    return

def test_change_message_visibility_to_large_value(sqs_extended_client_with_s3, queue, large_message_body, large_message_attribute):
    """
    Responsible for testing the entire workflow of sending, receiving, changing the message visibility and deleting a message
    using the SQS Extended Client through S3. The message visibility shall be changed to a large value which won't allow 
    it to be receivable from the queue temporarily.

    sqs_extended_client_with_s3: The SQS Extended Client with a S3 bucket created
    queue: The SQS Queue
    large_message_body: Message payload exceeding the limit of 256 KB
    large_message_attribute: Message attributes for the payload
    
    """
    logger.info("Initiating execution of test_change_message_visibility_to_large_value")

    initialize_extended_client_attributes_through_s3(sqs_extended_client_with_s3)
    perform_send_receive_delete_workflow(sqs_extended_client_with_s3, queue, large_message_body, copy.deepcopy(large_message_attribute), change_message_visibility=True, new_message_visibility=2400)
    assert is_s3_bucket_empty(sqs_extended_client_with_s3)

    logger.info("Completed execution of test_change_message_visibility_to_large_value")
    return



# Tests for change_message_visibility_batch

def test_change_message_visibility_batch_to_small_value(sqs_extended_client_with_s3, queue, large_message_body_entries):
    """
    Responsible for testing the entire workflow of sending, receiving, changing message visibilities and deleting
    large messages sent as a batch, through S3, using the SQS Extended Client.

    sqs_extended_client: The SQS Extended Client 
    queue: The SQS Queue
    large_message_body_entries: The batch entries consisting of large message bodies and small message attributes

    """  

    logger.info("Initiating execution of test_change_message_visibility_batch_to_small_value")

    initialize_extended_client_attributes_through_s3(sqs_extended_client_with_s3)
    perform_send_receive_delete_batch_workflow(sqs_extended_client_with_s3, queue, copy.deepcopy(large_message_body_entries), change_message_visibility=True, new_message_visibility=0)
    assert is_s3_bucket_empty(sqs_extended_client_with_s3)

    logger.info("Completed execution of test_change_message_visibility_batch_to_small_value")

    return

def test_change_message_visibility_batch_to_large_value(sqs_extended_client_with_s3, queue, large_message_body_entries):
    """
    Responsible for testing the entire workflow of sending, receiving, changing message visibilities and deleting
    large messages sent as a batch, through S3, using the SQS Extended Client.

    sqs_extended_client: The SQS Extended Client 
    queue: The SQS Queue
    large_message_body_entries: The batch entries consisting of large message bodies and small message attributes

    """  

    logger.info("Initiating execution of test_change_message_visibility_batch_to_large_value")

    initialize_extended_client_attributes_through_s3(sqs_extended_client_with_s3)
    perform_send_receive_delete_batch_workflow(sqs_extended_client_with_s3, queue, copy.deepcopy(large_message_body_entries), change_message_visibility=True, new_message_visibility=2400)
    assert is_s3_bucket_empty(sqs_extended_client_with_s3)

    logger.info("Completed execution of test_change_message_visibility_batch_to_large_value")

    return

