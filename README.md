# Amazon SQS Extended Client Library for Python

### Implements the functionality of [amazon-sqs-java-extended-client-lib](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-s3-messages.html) in Python

The Amazon SQS Extended Client allows clients to manage Amazon SQS message payloads that exceed the 256 KB message size limit, up to a size of 2 GB. In the event of publishing such large messages, the client accomplishes this feat by storing the actual payload in a S3 bucket and by storing the reference of the stored object in the SQS queue. Similarly, the extended-client is also used for retrieving and dereferencing these references of message objects stored in S3. Thus, the library is used for the following purposes:


1. Specify whether payloads are always stored in Amazon S3 or only when a payload's size exceeds 256 KB.
2. Send a message that references a single message object stored in an Amazon S3 bucket.
3. Get the corresponding payload object from an Amazon S3 bucket.
4. Delete the corresponding payload object from an Amazon S3 bucket.

## Message Attributes for the SQS Extended Client ##

The SQS Extended Client makes use of several additional message attributes which helps it to handle large message payloads. This section shall outline the various attributes present:

1. ```large_payload_support``` : This consists of the S3 Bucket name that is responsible for storing large messages
2. ```always_through_s3```: This decides if all the message should be serialized to the S3 bucket. If set to `False`, messages smaller than `256 KB` will not be serialized to the s3 bucket.
3. ```use_legacy_attribute```: This message attribute is present in the header for a message structure and is important for consumers to understand the name of the key in the dictionary which contains the information of the large message payload. If True, then all published messages use the Legacy reserved message attribute (```SQSLargePayloadSize```) instead of the current reserved message attribute (```ExtendedPayloadSize```).

## Global Variables used by the SQS Extended Client ##

1. ```MESSAGE_POINTER_CLASS```: The value held by this global variable, or by ```LEGACY_MESSAGE_POINTER_CLASS```, is critical to the functioning of the client as it holds the class name of the pointer that stored the original payload in a S3 bucket.
2. ```MAX_ALLOWED_ATTRIBUTES```: The value held by this global variable denotes the constraint of having a maximum of 10 message attributes for each large message payload.
3. ```S3_KEY_ATTRIBUTE_NAME```: The value held by this global variable denotes the S3 Key, if present, which would be used to store the large message payload.
3. ```DEFAULT_MESSAGE_SIZE_THRESHOLD```: This states the threshold for the size of the messages in the S3 bucket and it cannot be less than 0 or more than 262144 (default value).
4. ```RESERVED_ATTRIBUTE_NAME```: The value held by this global variable, or by ```LEGACY_RESERVED_ATTRIBUTE_NAME```, denotes the attribute name which will be reserved for the purpose of handling large message payloads.

## Getting Started

* **Sign up for AWS** -- Before you begin, you need an AWS account. For more information about creating an AWS account, see [create and activate aws account](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/).
* **Minimum requirements** -- Python 3.x (or later) and pip
* **Download** -- Download the latest preview release or pick it up from pip:
```
pip install amazon-sqs-extended-client
```


## Using the Extended Client

Ensure that the package has been imported in the file you want to run your code from, by doing:

### Setting up the prerequisites for sending message payloads > 256 KB

```
import boto3
import sqs_extended_client

sqs_extended_client = boto3.client("sqs", region_name="us-east-1")
sqs_extended_client.large_payload_support = "BUCKET_NAME_HERE" 
sqs_extended_client.use_legacy_attribute = False

# Creating a SQS Queue and extracting it's Queue URL
queue = sqs_extended_client.create_queue(
    QueueName = "DemoPreparationQueue"
)
queue_url = sqs_extended_client.get_queue_url(
    QueueName = "DemoPreparationQueue"
)['QueueUrl']

# Creating a S3 bucket using the S3 client 
sqs_extended_client.s3_client.create_bucket(Bucket=sqs_extended_client.large_payload_support)
```


### Enabling support for payloads > 256 KB

```
# Sending a large message

large_message = small_message * 300000 # Shall cross the limit of 256 KB

send_message_response = sqs_extended_client.send_message(
    QueueUrl=queue_url,
    MessageBody=large_message
)
assert send_message_response['ResponseMetadata']['HTTPStatusCode'] == 200
```

```
# Receiving the large message

receive_message_response = sqs_extended_client.receive_message(
    QueueUrl=queue_url,
    MessageAttributeNames=['All']
)
assert receive_message_response['Messages'][0]['Body'] == large_message
receipt_handle = receive_message_response['Messages'][0]['ReceiptHandle']
```

```
# Deleting the large message

# Set to True for deleting the payload from S3
sqs_extended_client.delete_payload_from_s3 = True 
delete_message_response = sqs_extended_client.delete_message(
    QueueUrl=queue_url,
    ReceiptHandle=receipt_handle
)

assert delete_message_response['ResponseMetadata']['HTTPStatusCode'] == 200
```

```
# Deleting the queue

delete_queue_response = sqs_extended_client.delete_queue(
    QueueUrl=queue_url
)

assert delete_queue_response['ResponseMetadata']['HTTPStatusCode'] == 200
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

