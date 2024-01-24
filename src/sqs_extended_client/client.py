from sqs_extended_client.exceptions import *
from sqs_extended_client.exception_messages import *
from json import dumps, loads
from uuid import uuid4
import boto3
import botocore.session

import logging
logger = logging.getLogger("sqs_extended_client.client")
logger.setLevel(logging.WARNING)

LEGACY_RESERVED_ATTRIBUTE_NAME = 'SQSLargePayloadSize'
RESERVED_ATTRIBUTE_NAME= 'ExtendedPayloadSize'

LEGACY_MESSAGE_POINTER_CLASS = 'com.amazon.sqs.javamessaging.MessageS3Pointer'
MESSAGE_POINTER_CLASS = 'software.amazon.payloadoffloading.PayloadS3Pointer'

S3_KEY_ATTRIBUTE_NAME = 'S3Key'
MAX_ALLOWED_ATTRIBUTES = 10 - 1  # 10 for SQS and 1 reserved attribute
DEFAULT_MESSAGE_SIZE_THRESHOLD = 262144

S3_BUCKET_NAME_MARKER = "-..s3BucketName..-"
S3_KEY_MARKER = "-..s3Key..-"

def _get_large_payload_support(self):
    return getattr(self, '_large_payload_support', False)

def _delete_large_payload_support(self):
    if hasattr(self, '_large_payload_support'):
        del self._large_payload_support
    
def _set_large_payload_support(self, bucket_name: str):
    assert isinstance(bucket_name, str) and bucket_name is not None
    self._large_payload_support = bucket_name

def _get_always_through_s3(self):
    return getattr(self, '_always_through_s3', False)

def _delete_always_through_s3(self):
    if hasattr(self, '_always_through_s3'):
        del self._always_through_s3
    
def _set_always_through_s3(self, value: bool):
    assert isinstance(value, bool)
    self._always_through_s3 = value

def _get_use_legacy_attribute(self):
    return getattr(self, '_use_legacy_attribute', False)

def _set_use_legacy_attribute(self, value: bool):
    assert isinstance(value, bool)
    self._use_legacy_attribute = value

def _delete_use_legacy_attribute(self):
    if hasattr(self, '_use_legacy_attribute'):
        del self._use_legacy_attribute
    

def _get_delete_payload_from_s3(self):
    return getattr(self, '_delete_payload_from_s3', False)

def _set_delete_payload_from_s3(self, value: bool):
    assert isinstance(value, bool)
    self._delete_payload_from_s3 = value

def _delete_delete_payload_from_s3(self):
    if hasattr(self, '_delete_payload_from_s3'):
        del self._delete_payload_from_s3

def _validate_required_parameters(self, required_params: list, error_message: str, **kwargs):
    """
    Responsible for checking if required parameters are present inside of 
    kwargs when calling API methods using the SQS Extended Client.

    required_params: The list of parameters required while making the specific API call
    error_message: The error message to pass when calling SQSExtendedClientException
    
    """

    for parameters in required_params:
        if parameters not in kwargs:
            raise SQSExtendedClientException(error_message)
        
    return



def _get_string_size_in_bytes(self, s: str) -> int:
    """
    Responsible for calculating the size of a string in bytes

    :s: The string whose size is to be determined.

    """
    return len(s.encode('utf-8'))

def _get_reserved_attribute_name_if_present(self, message_attributes: dict) -> str:
    """
    Responsible for checking the reserved message attribute, SQSLargePayloadSize
    or ExtendedPayloadSize in this specific case, exists in the 
    message_attributes.

    :message_attributes: A dictionary consisting of message attributes.
    Each message attribute consists of the name (key) along with a 
    type and value of the message body. The following types are supported
    for message attributes: StringValue, BinaryValue and DataType.
    """

    reserved_attribute_name = ""

    if RESERVED_ATTRIBUTE_NAME in message_attributes:
        reserved_attribute_name = RESERVED_ATTRIBUTE_NAME 
    elif LEGACY_RESERVED_ATTRIBUTE_NAME in message_attributes:
        reserved_attribute_name = LEGACY_RESERVED_ATTRIBUTE_NAME
    
    return reserved_attribute_name

def _get_s3_key(self, message_attributes: dict) -> str:
    """
    Responsible for checking if the S3 Key exists in the 
    message_attributes.

    :message_attributes: A dictionary consisting of message attributes.
    Each message attribute consists of the name (key) along with a 
    type and value of the message body. The following types are supported
    for message attributes: StringValue, BinaryValue and DataType.
    """

    if S3_KEY_ATTRIBUTE_NAME in message_attributes:
        return message_attributes[S3_KEY_ATTRIBUTE_NAME]["StringValue"]
    return str(uuid4())

def _get_message_attributes_size(self, message_attributes: dict) -> int:
    """
    Responsible for calculating the size, in bytes, of the message attributes
    of a given message payload.

    :message_attributes: A dictionary consisting of message attributes.
    Each message attribute consists of the name (key) along with a 
    type and value of the message body. The following types are supported
    for message attributes: StringValue, BinaryValue and DataType.

    """

    total_message_attributes_size = 0
    for key, value in message_attributes.items():
        total_message_attributes_size += self.get_string_size_in_bytes(key)

        datatype_value = value.get("DataType", 0)
        if datatype_value:
            total_message_attributes_size += self.get_string_size_in_bytes(datatype_value)
        
        stringtype_value = value.get("StringValue", 0)
        if stringtype_value:
            total_message_attributes_size += self.get_string_size_in_bytes(stringtype_value)
        
        binarytype_value = value.get("BinaryValue", 0)
        if binarytype_value:
            total_message_attributes_size += self.get_string_size_in_bytes(binarytype_value)
        
    return total_message_attributes_size


def _check_message_attributes(self, message_attributes: dict) -> None:
    """
    Responsible for checking the constraints on the message attributes
    for a given message.

    :message_attributes: A dictionary consisting of message attributes.
    Each message attribute consists of the name (key) along with a 
    type and value of the message body. The following types are supported
    for message attributes: StringValue, BinaryValue and DataType.

    """
    total_message_attributes_size = self.get_message_attributes_size(message_attributes)

    if total_message_attributes_size > DEFAULT_MESSAGE_SIZE_THRESHOLD:
        raise SQSExtendedClientException(ExceptionMessages.INVALID_MESSAGE_ATTRIBUTE_SIZE.format(total_message_attributes_size, 
        DEFAULT_MESSAGE_SIZE_THRESHOLD))
    
    message_attributes_num = len(message_attributes)
    if message_attributes_num > MAX_ALLOWED_ATTRIBUTES:
        raise SQSExtendedClientException(ExceptionMessages.INVALID_NUMBER_OF_MESSAGE_ATTRIBUTES.format(message_attributes_num, 
        MAX_ALLOWED_ATTRIBUTES))

    reserved_attribute_name = self.get_reserved_attribute_name_if_present(message_attributes)
    if reserved_attribute_name:
        raise SQSExtendedClientException(ExceptionMessages.INVALID_ATTRIBUTE_NAME_PRESENT.format(reserved_attribute_name))
    
    return

def _is_large(self, message_body: str, message_attributes: dict) -> bool:
    """
    Responsible for checking if the message payload exceeds the default message
    threshold.

    :message_body: A UTF-8 encoded version of the message body
    :message_attributes: A dictionary consisting of message attributes.
    Each message attribute consists of the name (key) along with a 
    type and value of the message body. The following types are supported
    for message attributes: StringValue, BinaryValue and DataType.

    """
    message_attribute_size = self.get_message_attributes_size(message_attributes)
    message_body_size = self.get_string_size_in_bytes(message_body)
    return (message_attribute_size + message_body_size > DEFAULT_MESSAGE_SIZE_THRESHOLD)


def _store_message_in_s3(self, message_body: str, message_attributes: dict) -> (str, dict):
    """
    Responsible for storing a message payload in a S3 Bucket

    :message_body: A UTF-8 encoded version of the message body
    :message_attributes: A dictionary consisting of message attributes.
    Each message attribute consists of the name (key) along with a 
    type and value of the message body. The following types are supported
    for message attributes: StringValue, BinaryValue and DataType.
    
    """

    if len(message_body) == 0:
        # Message cannot be empty
        raise SQSExtendedClientException(ExceptionMessages.INVALID_MESSAGE_BODY)

    if self.large_payload_support and (self.always_through_s3 or self.is_large(message_body, message_attributes)):
        
        # Check message attributes for ExtendedClient related constraints
        self.check_message_attributes(message_attributes)
        encoded_body = message_body.encode('utf-8')


        message_pointer = (
                LEGACY_MESSAGE_POINTER_CLASS if self.use_legacy_attribute else MESSAGE_POINTER_CLASS
            )
        
        attribute_name = (
                LEGACY_RESERVED_ATTRIBUTE_NAME if self.use_legacy_attribute else RESERVED_ATTRIBUTE_NAME
            )

        # Modifying the message attributes for storing it in the Queue
        message_attributes[attribute_name] = {}
        attribute_value = {"DataType": "Number", "StringValue": str(len(encoded_body))}
        message_attributes[attribute_name] = attribute_value

        # S3 Key should either be a constant or be a random uuid4 string. This was dervied from the test case found in the java repo.
        s3_key = self.get_s3_key(message_attributes)

        # Adding the object into the bucket
        self.s3_client.put_object(Body=encoded_body, Bucket=self.large_payload_support, Key=s3_key)

        # Modifying the message body for storing it in the Queue
        message_body = dumps(
            [
                message_pointer,
                {"s3BucketName": self.large_payload_support, "s3Key": s3_key},
            ]
        )
    
    return message_body, message_attributes


def _send_message_decorator(func):
    def send_message(*args, **kwargs):
        #The SQS Client will be the instance of the SQS class which calls the send_message function
        sqs_client = args[0] 
        required_params = ['QueueUrl', 'MessageBody']
        sqs_client.validate_required_parameters(required_params, ExceptionMessages.INVALID_ARGUMENTS_FOR_SEND_MESSAGE, **kwargs)

        
        kwargs["MessageBody"], kwargs["MessageAttributes"] = sqs_client.store_message_in_s3(kwargs["MessageBody"], kwargs.get("MessageAttributes", {}))
        response = func(*args, **kwargs)
        return response
            
    return send_message

def _embed_s3_pointer_in_receipt_handle(self, message: dict) -> str:
    """
    Responsible for modifying the receipt handle for messages that shall be stored in 
    S3. This will be critical for the functionality of delete_message which only 
    accepts a Queue URL and a receipt handle of a message whose retrieval is required.
    Since we won't have the message attributes of that message while calling delete_message,
    it is important to modify the receipt handle.

    :message: Represents each message response received after calling the `receive_message` API
    method of the SQS client. 

    """
    receipt_handle = message['ReceiptHandle']
    message_body = loads(message['Body'])

    if not (isinstance(message_body, list) and len(message_body) == 2 and isinstance(message_body[1], dict)):
        raise SQSExtendedClientException(ExceptionMessages.INVALID_FORMAT_WHEN_RETRIEVING_STORED_S3_MESSAGES)
    
    s3_message_bucket_name = message_body[1]['s3BucketName']
    s3_message_key = message_body[1]['s3Key']

    modified_receipt_handle = S3_BUCKET_NAME_MARKER + s3_message_bucket_name + S3_BUCKET_NAME_MARKER + S3_KEY_MARKER + s3_message_key + S3_KEY_MARKER + receipt_handle

    return modified_receipt_handle

def _is_s3_receipt_handle(self, receipt_handle: str) -> bool:
    """
    Responsible for checking if a receipt handle has been modified to store the 
    message in S3.

    :receipt_handle:  Represents the receipt handle of message.

    """
    return S3_BUCKET_NAME_MARKER in receipt_handle and S3_KEY_MARKER in receipt_handle

def _get_original_receipt_handle(self, modified_receipt_handle: str) -> str:
    """
    Responsible for returning the receipt handle of the message before 
    it was modified.

    :modified_receipt_handle: Represents the receipt handle which was modified
    when the message was stored in S3.

    """
    return modified_receipt_handle.split('-')[-1]


def _retrieve_message_from_s3(self, message_body: str) -> str:
    """
    Responsible for retrieving a message payload from a S3 Bucket, if it exists.

    :message_body: A string containing the first element to be the S3 class pointer
    and the second element to be a dictionary consisting of the s3BucketName and 
    the s3Key for the bucket.
    
    """
    message_body = loads(message_body)


    if not (isinstance(message_body, list) and len(message_body) == 2 and isinstance(message_body[1], dict)):
        raise SQSExtendedClientException(ExceptionMessages.INVALID_FORMAT_WHEN_RETRIEVING_STORED_S3_MESSAGES)
    
    s3_pointer = message_body[0]
    s3_details = message_body[1]

    s3_bucket_name, s3_key = s3_details['s3BucketName'], s3_details['s3Key']

    response = self.s3_client.get_object(Bucket=s3_bucket_name, Key=s3_key)

    # The message body is under a wrapper class called StreamingBody
    streaming_body = response['Body']
    message_body = streaming_body.read().decode()

    return message_body

def _get_from_receipt_handle_by_marker(self, modified_receipt_handle: str, marker: str) -> str:
    """
    Responsible for returning the substring between a given marker in a modified receipt 
    handle.

    :modified_receipt_handle: Represents the receipt handle which was modified
    when the message was stored in S3.

    :marker: Represents a marker present while modifying the original receipt handle.

    """
    first_occur = modified_receipt_handle.find(marker)
    second_occur = modified_receipt_handle.find(marker, first_occur + len(marker))

    return modified_receipt_handle[first_occur + len(marker):second_occur]

def _receive_message_decorator(func):
    def receive_message(*args, **kwargs):
        sqs_client = args[0]
        required_params = ['QueueUrl']
        sqs_client.validate_required_parameters(required_params, ExceptionMessages.INVALID_ARGUMENTS_FOR_RECEIVE_MESSAGE, **kwargs)

        requested_attributes = kwargs.get("MessageAttributeNames", [])

        if not {'All', '.*', RESERVED_ATTRIBUTE_NAME, LEGACY_RESERVED_ATTRIBUTE_NAME}.intersection(requested_attributes):
            if sqs_client.use_legacy_attribute:
                requested_attributes.append(LEGACY_RESERVED_ATTRIBUTE_NAME)
            else:
                requested_attributes.append(RESERVED_ATTRIBUTE_NAME)

        kwargs['MessageAttributeNames'] = requested_attributes.copy()
        
        response = func(*args, **kwargs)
        messages = response.get('Messages', [])

        for message in messages:
            message_attributes = message.get('MessageAttributes', {})

            # Checking if the RESERVED_ATTRIBUTE_NAME exists as a key
            if RESERVED_ATTRIBUTE_NAME in message_attributes or LEGACY_RESERVED_ATTRIBUTE_NAME in message_attributes:

                # Embed s3 object pointer in the receipt handle.
                modified_receipt_handle = sqs_client.embed_s3_pointer_in_receipt_handle(message)
                message['ReceiptHandle'] = modified_receipt_handle
                
                # Retrieve the original payload from S3!
                modified_message_body = message['Body']
                message_body = sqs_client.retrieve_message_from_s3(modified_message_body)
                
                # Overwriting the message body
                message['Body'] = message_body

                # Removing the required message attribute
                message_attributes.pop(RESERVED_ATTRIBUTE_NAME, None) or message_attributes.pop(LEGACY_RESERVED_ATTRIBUTE_NAME, None)

        return response
    return receive_message


def _delete_message_from_s3(self, modified_receipt_handle: str) -> str:
    s3_bucket_name = self.get_from_receipt_handle_by_marker(modified_receipt_handle, S3_BUCKET_NAME_MARKER)
    s3_key = self.get_from_receipt_handle_by_marker(modified_receipt_handle, S3_KEY_MARKER)

    response = self.s3_client.delete_object(
        Bucket=s3_bucket_name,
        Key=s3_key
    )

    #Check if the delete operation succeeded, if not raise an error?
    status_code = response['ResponseMetadata']['HTTPStatusCode']
    if status_code != 204:
        raise SQSExtendedClientServiceException(ExceptionMessages.FAILED_DELETE_MESSAGE_FROM_S3.format(status_code))

    return response


def _delete_message_decorator(func):
    def delete_message(*args, **kwargs):
        sqs_client = args[0]
        required_params = ['QueueUrl', 'ReceiptHandle']

        sqs_client.validate_required_parameters(required_params, ExceptionMessages.INVALID_ARGUMENTS_FOR_DELETE_MESSAGE, **kwargs)

        queue_url = kwargs['QueueUrl']
        receipt_handle = kwargs['ReceiptHandle']

        if sqs_client.is_s3_receipt_handle(receipt_handle):
        
            if sqs_client.delete_payload_from_s3:
                sqs_client.delete_message_from_s3(receipt_handle)
            
            kwargs['ReceiptHandle'] = sqs_client.get_original_receipt_handle(receipt_handle)
        
        response = func(*args, **kwargs)
        return response

    return delete_message


def _send_message_batch_decorator(func):
    def send_message_batch(*args, **kwargs):
        sqs_client = args[0]

        required_params = ['QueueUrl', 'Entries']
        sqs_client.validate_required_parameters(required_params, ExceptionMessages.INVALID_ARGUMENTS_FOR_SEND_MESSAGE_BATCH, **kwargs)

        queue_url = kwargs['QueueUrl']
        message_entries = kwargs['Entries']
        
        if sqs_client.large_payload_support:
            for message_entry in message_entries:
                message_entry["MessageBody"], message_entry["MessageAttributes"] = sqs_client.store_message_in_s3(message_entry["MessageBody"], message_entry.get("MessageAttributes", {}))

        response = func(*args, **kwargs)
        return response

    return send_message_batch

def _delete_message_batch_decorator(func):
    def delete_message_batch(*args, **kwargs):
        sqs_client = args[0]

        required_params = ['QueueUrl', 'Entries']
        sqs_client.validate_required_parameters(required_params, ExceptionMessages.INVALID_ARGUMENTS_FOR_DELETE_MESSAGE_BATCH, **kwargs)

        queue_url = kwargs['QueueUrl']
        message_entries = kwargs['Entries']

        if sqs_client.large_payload_support:
            for message_entry in message_entries:
                receipt_handle = message_entry['ReceiptHandle']
                
                if sqs_client.is_s3_receipt_handle(receipt_handle):

                    if sqs_client.delete_payload_from_s3:
                        sqs_client.delete_message_from_s3(receipt_handle)  

                    message_entry['ReceiptHandle'] = sqs_client.get_original_receipt_handle(receipt_handle)
                 
        response = func(*args, **kwargs)
        return response
    
    return delete_message_batch


def _change_message_visibility_decorator(func):
    def change_message_visibility(*args, **kwargs):
        sqs_client = args[0]

        required_params = ['QueueUrl', 'ReceiptHandle', 'VisibilityTimeout']
        sqs_client.validate_required_parameters(required_params, ExceptionMessages.INVALID_ARGUMENTS_FOR_CHANGE_MESSAGE_VISIBILITY, **kwargs)

        receipt_handle = kwargs['ReceiptHandle']
    
        if sqs_client.is_s3_receipt_handle(receipt_handle):
            kwargs['ReceiptHandle'] = sqs_client.get_original_receipt_handle(receipt_handle)
        
        response = func(*args, **kwargs)
        return response

    return change_message_visibility

def _change_message_visibility_batch_decorator(func):
    def change_message_visibility_batch(*args, **kwargs):
        sqs_client = args[0]

        required_params = ['QueueUrl', 'Entries']
        sqs_client.validate_required_parameters(required_params, ExceptionMessages.INVALID_ARGUMENTS_FOR_CHANGE_MESSAGE_VISIBILITY_BATCH, **kwargs)

        message_entries = kwargs['Entries']

        for message in message_entries:
            receipt_handle = message['ReceiptHandle']
            if sqs_client.is_s3_receipt_handle(receipt_handle):
                message['ReceiptHandle'] = sqs_client.get_original_receipt_handle(receipt_handle)
        
        response = func(*args, **kwargs)
        return response
    
    return change_message_visibility_batch

def _purge_queue_decorator(func):
    def purge_queue(*args, **kwargs):
        sqs_client = args[0]

        required_params = ['QueueUrl']
        sqs_client.validate_required_parameters(required_params, ExceptionMessages.INVALID_ARGUMENTS_FOR_PURGE_QUEUE, **kwargs)

        logger.warning("Calling purgeQueue deletes SQS messages without deleting their payload from S3.")

        response = func(*args, **kwargs)
        return response
    return purge_queue


class SQSExtendedClientSession(boto3.session.Session):
    """ 
    A session stores configuration state and allows you to create service
    clients and resources. SQSExtendedClientSession extends the functionality 
    of the boto3 Session object by using the .register event functionality. 
        
    :type aws_access_key_id: string
    :param aws_access_key_id: AWS access key ID
    :type aws_secret_access_key: string
    :param aws_secret_access_key: AWS secret access key
    :type aws_session_token: string
    :param aws_session_token: AWS temporary session token
    :type region_name: string
    :param region_name: Default region when creating new connections
    :type botocore_session: botocore.session.Session
    :param botocore_session: Use this Botocore session instead of creating
                             a new default one.
    :type profile_name: string
    :param profile_name: The name of a profile to use. If not given, then
                         the default profile is used.
            
    """

    def __init__(
        self,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        aws_session_token=None,
        region_name=None,
        botocore_session=None,
        profile_name=None,
    ):
        
        if botocore_session is None:
            self._session = botocore.session.get_session()
        else:
            self._session = botocore_session

        self.add_custom_user_agent()

        # Calling the init function of the boto3 class.
        super().__init__(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        region_name=region_name,
        botocore_session=self._session,
        profile_name=profile_name
        )
        
        self.events.register('creating-client-class.sqs', self.add_client_attributes)
    

    def add_custom_user_agent(self):
        # Attaching SQSExtendedClientSession to the HTTP headers

        user_agent_header = self.__class__.__name__

        # Attaching SQSExtendedClientSession to the HTTP headers
        if self._session.user_agent_extra:
            self._session.user_agent_extra += " " + user_agent_header
        else:
            self._session.user_agent_extra = user_agent_header
    

    def add_client_attributes(self, class_attributes, **kwargs):
        # Adding additional functionality to the SQS client.

        # Defining properties for required attributes of the client
        class_attributes["large_payload_support"] = property(
            _get_large_payload_support,
            _set_large_payload_support,
            _delete_large_payload_support
        )

        class_attributes["always_through_s3"] = property(
            _get_always_through_s3, 
            _set_always_through_s3, 
            _delete_always_through_s3
        )

        class_attributes["use_legacy_attribute"] = property(
            _get_use_legacy_attribute,
            _set_use_legacy_attribute,
            _delete_use_legacy_attribute
        )

        class_attributes["delete_payload_from_s3"] = property(
            _get_delete_payload_from_s3,
            _set_delete_payload_from_s3,
            _delete_delete_payload_from_s3
        )

        class_attributes["get_string_size_in_bytes"] = _get_string_size_in_bytes
        class_attributes["get_reserved_attribute_name_if_present"] = _get_reserved_attribute_name_if_present
        class_attributes["get_s3_key"] = _get_s3_key
        class_attributes["get_message_attributes_size"] = _get_message_attributes_size
        class_attributes["check_message_attributes"] = _check_message_attributes
        class_attributes["is_large"] = _is_large
        class_attributes["store_message_in_s3"] = _store_message_in_s3
        class_attributes["retrieve_message_from_s3"] = _retrieve_message_from_s3
        class_attributes["delete_message_from_s3"] = _delete_message_from_s3
        class_attributes["validate_required_parameters"] = _validate_required_parameters

        class_attributes["embed_s3_pointer_in_receipt_handle"] = _embed_s3_pointer_in_receipt_handle
        class_attributes["is_s3_receipt_handle"] = _is_s3_receipt_handle
        class_attributes["get_original_receipt_handle"] = _get_original_receipt_handle
        class_attributes["get_from_receipt_handle_by_marker"] = _get_from_receipt_handle_by_marker

        # Adding the S3 client to the object
        class_attributes["s3_client"] = boto3.client("s3")

        # overwriting the send_message function
        class_attributes["send_message"] = _send_message_decorator(class_attributes["send_message"])

        # overwriting the receive_message function
        class_attributes["receive_message"] = _receive_message_decorator(class_attributes["receive_message"])

        # overwriting the delete_message function
        class_attributes["delete_message"] = _delete_message_decorator(class_attributes["delete_message"])

        # overwriting the send_message_batch function
        class_attributes["send_message_batch"] = _send_message_batch_decorator(class_attributes["send_message_batch"])

        # overwriting the delete_message_batch function
        class_attributes["delete_message_batch"] = _delete_message_batch_decorator(class_attributes["delete_message_batch"])

        # overwriting the change_message_visibility function
        class_attributes["change_message_visibility"] = _change_message_visibility_decorator(class_attributes["change_message_visibility"])

        # overwriting the change_message_visibility_batch function
        class_attributes["change_message_visibility_batch"] = _change_message_visibility_batch_decorator(class_attributes["change_message_visibility_batch"])

        # overwriting the purge_queue function
        class_attributes["purge_queue"] = _purge_queue_decorator(class_attributes["purge_queue"])
