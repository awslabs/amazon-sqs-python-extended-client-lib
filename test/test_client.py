import pytest
import unittest
import boto3
import os
import random
import string
import copy

from moto import mock_sqs, mock_s3
from json import loads, dumps
from unittest.mock import create_autospec

from sqs_extended_client.exceptions import *
from sqs_extended_client.exception_messages import *
from sqs_extended_client.client import SQSExtendedClientSession, DEFAULT_MESSAGE_SIZE_THRESHOLD, MAX_ALLOWED_ATTRIBUTES, LEGACY_MESSAGE_POINTER_CLASS, LEGACY_RESERVED_ATTRIBUTE_NAME, RESERVED_ATTRIBUTE_NAME, MESSAGE_POINTER_CLASS


# Helpers for the test cases
def _message_body_builder(is_legacy: bool, s3_key: str, s3_bucket_name: str) -> str:
    """
    Responsible for developing a message payload which has the original contents
    stored in a S3 bucket.

    :is_legacy: denotes if the legacy message pointer class should be used
    :s3_key: denotes the S3 Key for the bucket
    :s3_bucket_name: denotes the S3 Bucket name

    """
    message_pointer = LEGACY_MESSAGE_POINTER_CLASS if is_legacy else MESSAGE_POINTER_CLASS

    message_body = dumps(
        [
            message_pointer,
            {"s3BucketName": s3_bucket_name, "s3Key": s3_key},
        ]
    )
    return message_body

def _compare_json_objects(json1: str, json2: str) -> bool:

    """
    Responsible for comparing two JSON strings by first converting them into Python
    Objects

    :json1: a JSON string representing a modified message payload
    :json2: a JSON string representing a modified message payload

    """

    json_object_1 = loads(json1)
    json_object_2 = loads(json2)

    return json_object_1 == json_object_2

def _parse_receive_message_response(response: dict) -> (str, str, dict, dict):
    """

    Responsible for parsing through the resposne object received after calling
    the sqs_extended_client.receive_message method. Returns the message body, 
    the modified receipt handle, the message attributes and the response metadata 
    all of which are stored inside the response dictionary.

    :response: represents the response body received after calling 
    sqs_extended_client.receive_message.

    """
    message = response['Messages'][0]
    response_metadata = response['ResponseMetadata']

    returned_message_body = message['Body']
    returned_message_attributes = message['MessageAttributes']
    modified_receipt_handle = message['ReceiptHandle']

    return (returned_message_body, modified_receipt_handle, returned_message_attributes, response_metadata)


class TestSQSExtendedClientErrors(unittest.TestCase):
    """ 
    TestSQSExtendedClientErrors tests the error subclasses
    defined in exceptions.py 
    
    """
    
    def test_sqs_extended_client_exception(self):

        error = "Undeclared/Missing S3 bucket name for payload offloading!"

        with self.assertRaises(SQSExtendedClientException) as context:
            raise SQSExtendedClientException(error)

        self.assertEqual(str(context.exception), error)
    
    
    def test_sqs_extended_client_service_exception(self):

        error = {
            'Error': {
                'Code': 'NoSuchBucket',
                'Message': 'The specified bucket does not exist',
                'BucketName': 'my-bucket',
                'Key': 'my-object-key'
                }
            }

        error_response = "An error occurred (NoSuchBucket) when calling the s3 operation: The specified bucket does not exist"

        with self.assertRaises(SQSExtendedClientServiceException) as context:
            raise SQSExtendedClientServiceException(error, 's3')

        self.assertEqual(str(context.exception), error_response)  


class TestSQSExtendedClient(unittest.TestCase):
    """ 
    TestSQSExtendedClient tests the SQS Extended Client
    
    """
    
    @classmethod
    def setUpClass(cls):
        """
        Setting up resources for the entire test class

        """
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
        cls.bucket_name = "test-bucket"
        cls.queue_name = "test-queue"

        cls.mock_sqs = mock_sqs()
        cls.mock_sqs.start()
        
        cls.sqs = boto3.client('sqs', region_name=os.environ["AWS_DEFAULT_REGION"])
        cls.sqs_queue_url = cls.sqs.create_queue(QueueName=cls.queue_name)["QueueUrl"]


    def initialize_extended_client_setup(self):
        """
        Setting up instance properties for an object of the SQSExtendedClientSession.

        """
        self.sqs_extended_client = SQSExtendedClientSession().client("sqs")
        self.sqs_extended_client.large_payload_support = "test-bucket"
        
        self.sqs_extended_client.always_through_s3 = True
        self.sqs_extended_client.use_legacy_attribute = False
    
    def initialize_test_properties(self):
        """
        Setting up test attributes which shall be used by the test cases.

        """

        self.s3_resource = boto3.resource("s3", region_name=os.environ["AWS_DEFAULT_REGION"])
        self.s3_resource.create_bucket(Bucket=TestSQSExtendedClient.bucket_name)

        self.s3_key  = 'cc562f4d-c6f3-4b0c-bf29-007a902c391f'
        self.receipt_handle = 'x' * 100

        self.large_message_body = "s" * DEFAULT_MESSAGE_SIZE_THRESHOLD
        self.large_message_attribute = {
            'Large_Message_Attribute': {
                'StringValue': self.large_message_body,
                'DataType': 'String'
            }
        }
        self.small_message_body = "s"
        self.small_message_attribute = {
            'Small_Message_Attribute': {
                'StringValue': self.small_message_body,
                'DataType': 'String'
            }
        }

        self.reserved_message_attribute = self.small_message_attribute.copy()
        self.reserved_message_attribute[RESERVED_ATTRIBUTE_NAME] = {
                        'StringValue': '262145',
                        'DataType': 'Number'
                    }

        # Developing a message attribute with 11 different attributes.
        self.excess_number_of_attributes = {}
        self.message_attributes_num = 11
        for i in range(11):
            attribute_name = f"Attribute {i+1}"
            attribute_value = f"Value {i+1}"

            attribute_data = {
                'StringValue': attribute_value,
                'DataType': 'String',
            }
            self.excess_number_of_attributes[attribute_name] = attribute_data
        
        # Developing a message attribute with a S3 Key
        self.message_attributes_with_s3_key = self.small_message_attribute.copy()
        self.message_attributes_with_s3_key['S3Key'] = {
                'StringValue': self.s3_key,
                'DataType': 'String'
            }
        
        # Developing a message attribute without a S3 Key
        self.message_attributes_without_s3_key = self.small_message_attribute.copy()

        self.message_attribute_with_queue_url = self.small_message_attribute.copy()
        self.message_attribute_with_queue_url["QueueUrl"] = TestSQSExtendedClient.sqs_queue_url
    
        # Creating a copy of the message attributes since a new key, LEGACY_RESERVED_ATTRIBUTE_NAME or
        # RESERVED_ATTRIBUTE_NAME, will be added during the `send_message` call. This is useful 
        # for all the tests testing receive_message. 
        self.unmodified_message_attribute = self.message_attributes_with_s3_key.copy()


        self.small_batch_entries = [
            {
                'Id': f"ID-{random.randint(0,10000)}",
                'MessageBody': self.small_message_body,
                'MessageAttributes': self.small_message_attribute.copy()
            },
            {
                'Id': f"ID-{random.randint(0,10000)}",
                'MessageBody': self.small_message_body,
                'MessageAttributes': self.small_message_attribute.copy()

            }
        ]

        self.large_batch_entries = [
            {
                'Id': f"ID-{random.randint(0,10000)}",
                'MessageBody': self.large_message_body,
                'MessageAttributes': self.small_message_attribute.copy()
            },
            {
                'Id': f"ID-{random.randint(0,10000)}",
                'MessageBody': self.small_message_body,
                'MessageAttributes': self.small_message_attribute.copy()

            }
        ]        

    @mock_s3
    def setUp(self):
        """
        Setting up resources which shall be used by the defined test cases

        """
        self.initialize_extended_client_setup()
        self.initialize_test_properties()
    
    def tearDown(self):
        """
        Shutting down resources after each test run

        """
        self.sqs_extended_client.purge_queue(QueueUrl=TestSQSExtendedClient.sqs_queue_url)        
        del self.sqs_extended_client

            
    @classmethod
    def tearDownClass(cls):
        # Shutting down resources for the entire test class
        cls.mock_sqs.stop()

    def _send_and_receive_message(self, message_body, message_attributes):
        """

        Helper function to send and receive a message using the SQS Extended Client.
        Returns the message body, the modified receipt handle, the message attributes and the response metadata 
        which are received after calling sqs_extended_client.receive_message.

        :message_body: The message body of the payload.
        :message_attributes: The message attributes of the payload.

        """

        send_message_response = self.sqs_extended_client.send_message(QueueUrl=TestSQSExtendedClient.sqs_queue_url, 
            MessageBody=message_body, MessageAttributes=message_attributes)
        self.assertEqual(send_message_response['ResponseMetadata']['HTTPStatusCode'], 200)

        receive_message_response = self.sqs_extended_client.receive_message(QueueUrl=TestSQSExtendedClient.sqs_queue_url, MaxNumberOfMessages=1, MessageAttributeNames=['All'])

        return _parse_receive_message_response(receive_message_response)


    def _send_and_receive_batch_messages(self, entries: list) -> dict:

        send_message_batch_response = self.sqs_extended_client.send_message_batch(
            QueueUrl=TestSQSExtendedClient.sqs_queue_url, 
            Entries=copy.deepcopy(entries)
        )

        self.assertEqual(send_message_batch_response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertEqual(len(send_message_batch_response['Successful']), len(entries))

        receive_message_batch_response = self.sqs_extended_client.receive_message(
            QueueUrl=TestSQSExtendedClient.sqs_queue_url,
            MaxNumberOfMessages=2,
            WaitTimeSeconds=10,
            MessageAttributeNames=['All']
        )

        messages = receive_message_batch_response['Messages']

        self.assertEqual(receive_message_batch_response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertEqual(len(messages), len(entries))

        return messages


    def _build_batch_entries(self, messages: list, entries: list, visibility_timeout = None) -> list:
        
        batch_entries = []

        for pos, message in enumerate(messages):
            self.assertEqual(message['Body'], entries[pos]['MessageBody'])

            input = {
                "Id": f"ID-{random.randint(0,10000)}",
                "ReceiptHandle": message['ReceiptHandle']
            }

            if visibility_timeout is not None:
                input["VisibilityTimeout"] = visibility_timeout

            batch_entries.append(input)
        
        return batch_entries

    def _change_message_visibility_helper(self, sqs_extended_client, visibility_timeout: int, message_body: str, message_attributes: dict):
        """
        Acts as a helper for testers testing the functionality of change_message_visibility.
        Sends, receives and changes the visibility of messages according to the given input.

        sqs_extended_client: The SQS Extended Client 
        visibility_timeout: The new visibility timeout
        message_body: The message body
        message_attributes: The message attributes

        """
        _, receipt_handle, _, _ = \
            self._send_and_receive_message(message_body, message_attributes.copy())
        
        response = self.sqs_extended_client.change_message_visibility(
            QueueUrl=TestSQSExtendedClient.sqs_queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=visibility_timeout
        )

        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        
        return

    def _change_message_visibility_batch_helper(self, sqs_extended_client, input_entries, visibility_timeout):
        """
        Acts as a helper for testers testing the functionality of change_message_visibility_batch.
        Sends, receives and changes the visibility of messages according to the given input.

        sqs_extended_client: The SQS Extended Client 
        visibility_timeout: The new visibility timeout
        input_entries: The entries consisting of different message payloads

        """

        messages = self._send_and_receive_batch_messages(copy.deepcopy(input_entries))
        entries = self._build_batch_entries(messages, copy.deepcopy(input_entries), visibility_timeout)

        change_visibility_response = self.sqs_extended_client.change_message_visibility_batch(
            QueueUrl=TestSQSExtendedClient.sqs_queue_url,
            Entries=entries
        )

        self.assertEqual(change_visibility_response['ResponseMetadata']['HTTPStatusCode'], 200)

        receive_message_response = self.sqs_extended_client.receive_message(
            QueueUrl=TestSQSExtendedClient.sqs_queue_url,
            MaxNumberOfMessages=2,
            WaitTimeSeconds=10
        )

        self.assertEqual(receive_message_response['ResponseMetadata']['HTTPStatusCode'], 200)

    def test_default_session_is_extended_sqs_client_session(self):
        """
        Test to verify if the boto3 session has been replaced
        
        """
        assert boto3.session.Session == SQSExtendedClientSession
    
    def test_custom_user_agent(self):
        class_obj = SQSExtendedClientSession()
        assert class_obj.__class__.__name__ in class_obj._session.user_agent()

    def test_get_string_size_in_bytes(self):

        test_cases = [
            ("", 0),
            ("SQS", 3),
            ("SQSExtendedClient", 17),
            ("S#!", 3)
        ]

        for string, expected_length in test_cases:
            length = self.sqs_extended_client.get_string_size_in_bytes(string)
            self.assertEqual(length, expected_length)
    
    def test_get_reserved_attribute_name_if_present(self):

        test_cases = [
            (
                {
                    'Author': {
                        'StringValue': 'Sam',
                        'DataType': 'String'
                    },
                    'ExtendedPayloadSize': {
                        'StringValue': '262145',
                        'DataType': 'Number'
                    }
                },
                "ExtendedPayloadSize"
            ),
            (
                {
                    'Author': {
                        'StringValue': 'Sam',
                        'DataType': 'String'
                    },
                    'SQSLargePayloadSize': {
                        'StringValue': '262145',
                        'DataType': 'Number'
                    }
                },
                "SQSLargePayloadSize"
            ),
            (
                {
                    'Author': {
                        'StringValue': 'Sam',
                        'DataType': 'String'
                    },
                },
                ""
            ),
        ]

        

        for message_attributes, expected_value in test_cases:
            returned_value = self.sqs_extended_client.get_reserved_attribute_name_if_present(message_attributes)
            self.assertEqual(expected_value, returned_value)
    
    def test_get_s3_key(self):
        
        returned_value_without_s3_key = self.sqs_extended_client.get_s3_key(self.message_attributes_without_s3_key)
        returned_value_with_s3_key = self.sqs_extended_client.get_s3_key(self.message_attributes_with_s3_key)

        self.assertRegex(returned_value_without_s3_key, r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
        self.assertEqual(self.s3_key, returned_value_with_s3_key)

    def test_get_message_attributes_size(self):

        test_cases = [
            (
                {},
                0
            ),
            (
                self.small_message_attribute,
                30
            ),
            (
                self.large_message_attribute,
                262173
            )
        ]

        for message_attribute, expected_byte_size in test_cases:
            returned_byte_size = self.sqs_extended_client.get_message_attributes_size(message_attribute)
            self.assertEqual(expected_byte_size, returned_byte_size)
    
    def test_check_message_attributes(self):

        # Error message descriptions:
        large_message_attribute_size = 262173
        large_message_attribute_size_error = """Total size of message attributes is {0} bytes which is larger than the threshold of {1} bytes. Consider including the payload in the message body instead of the message attributes. """.format(large_message_attribute_size, 
            DEFAULT_MESSAGE_SIZE_THRESHOLD)
        
        reserved_attribute_name = "ExtendedPayloadSize"
        reserved_attribute_error = """Message attribute name {0} is reserved for use by the SQS extended client. """.format(reserved_attribute_name)
        
        excess_number_of_attributes_error = """Number of message attributes {0} exceeds the maximum allowed for large-payload messages {1} """.format(self.message_attributes_num, 
            MAX_ALLOWED_ATTRIBUTES)
        

        
        test_cases = [
            (
                self.large_message_attribute,
                large_message_attribute_size_error
            ),
            (
                self.reserved_message_attribute,
                reserved_attribute_error,
            ),
            (
                self.excess_number_of_attributes,
                excess_number_of_attributes_error
            )

        ]

        for message_attribute, expected_error in test_cases:
            with self.assertRaises(SQSExtendedClientException) as context:
                self.sqs_extended_client.check_message_attributes(message_attribute)

            self.assertEqual(str(context.exception), expected_error)

    
    def test_is_large(self):

        test_cases = [
            (
                self.large_message_body,
                self.large_message_attribute,
                True
            ),
            (
                self.small_message_body,
                self.small_message_attribute,
                False
            )
        ]

        for message_body, message_attribute, expected_value in test_cases:
            self.assertEqual(self.sqs_extended_client.is_large(message_body,message_attribute), expected_value)
        
    def test_store_in_s3_invalid_payload(self):
        message_body = ""
        with pytest.raises(SQSExtendedClientException) as error:
            self.sqs_extended_client.store_message_in_s3(message_body, self.small_message_attribute)
        
        self.assertEqual(str(error.value), ExceptionMessages.INVALID_MESSAGE_BODY)


    def test_store_in_s3_small_payload_without_always_through_s3(self):
        self.sqs_extended_client.always_through_s3 = False

        message_body, message_attribute = self.sqs_extended_client.store_message_in_s3(self.small_message_body, self.small_message_attribute)

        self.assertEqual(message_body, self.small_message_body)
        self.assertEqual(message_attribute, self.small_message_attribute)
    
    def test_store_in_s3_small_payload_without_setting_always_through_s3_and_large_payload_support(self):
        del self.sqs_extended_client.always_through_s3
        del self.sqs_extended_client.large_payload_support

        message_body, message_attribute = self.sqs_extended_client.store_message_in_s3(self.small_message_body, self.small_message_attribute)

        self.assertEqual(message_body, self.small_message_body)
        self.assertEqual(message_attribute, self.small_message_attribute)
    
    def test_store_in_s3_small_payload_without_always_through_s3_and_without_setting_use_legacy_attribute(self):
        del self.sqs_extended_client.use_legacy_attribute
        self.sqs_extended_client.always_through_s3 = False

        message_body, message_attribute = self.sqs_extended_client.store_message_in_s3(self.small_message_body, self.small_message_attribute)

        self.assertEqual(message_body, self.small_message_body)
        self.assertEqual(message_attribute, self.small_message_attribute)

    
    def test_store_in_s3_small_payload_with_always_through_s3(self):
        
        message_body, message_attribute = self.sqs_extended_client.store_message_in_s3(self.small_message_body, self.message_attributes_with_s3_key)

        # Asserting the stored value in the S3 Bucket
        stored_message_body = self.s3_resource.Object(TestSQSExtendedClient.bucket_name, self.s3_key).get()["Body"].read().decode("utf-8")
        self.assertEqual(stored_message_body, self.small_message_body)

        # Asserting that a new message attribute has been added
        self.assertEqual(message_attribute[RESERVED_ATTRIBUTE_NAME], {"DataType": "Number", "StringValue": str(len(stored_message_body.encode("utf-8")))})

        # Asserting that the message payload has been modified
        expected_message_body = _message_body_builder(self.sqs_extended_client.use_legacy_attribute, self.s3_key, TestSQSExtendedClient.bucket_name)
        assert _compare_json_objects(message_body, expected_message_body), True

    
    def test_store_in_s3_large_payload(self):
        
        message_body, message_attribute = self.sqs_extended_client.store_message_in_s3(self.large_message_body, self.message_attributes_with_s3_key)

        # Asserting the stored value in the S3 Bucket
        stored_message_body = self.s3_resource.Object(TestSQSExtendedClient.bucket_name, self.s3_key).get()["Body"].read().decode("utf-8")
        assert stored_message_body == self.large_message_body

        # Asserting that a new message attribute has been added
        self.assertEqual(message_attribute[RESERVED_ATTRIBUTE_NAME], {"DataType": "Number", "StringValue": str(len(self.large_message_body.encode("utf-8")))})

        # Asserting that the message payload has been modified
        expected_message_body = _message_body_builder(self.sqs_extended_client.use_legacy_attribute, self.s3_key, TestSQSExtendedClient.bucket_name)
        assert _compare_json_objects(message_body, expected_message_body), True
    
    def test_send_message_with_invalid_arguments(self):
        with pytest.raises(SQSExtendedClientException) as error:
            self.sqs_extended_client.send_message()
        
        self.assertEqual(str(error.value), ExceptionMessages.INVALID_ARGUMENTS_FOR_SEND_MESSAGE)
        
    def test_send_message_with_large_payload(self):

        mocked_message_body =  _message_body_builder(False, self.s3_key,TestSQSExtendedClient.bucket_name)
        mocked_message_attribute = {}

        mocked_message_attribute[RESERVED_ATTRIBUTE_NAME] = {"DataType": "Number", "StringValue": str(len(self.large_message_body.encode("utf-8")))}

        store_message_in_s3_mock = create_autospec(
            self.sqs_extended_client.store_message_in_s3,
            return_value=(mocked_message_body, mocked_message_attribute)
        )

        self.sqs_extended_client.store_message_in_s3 = store_message_in_s3_mock

        self.sqs_extended_client.send_message(QueueUrl=TestSQSExtendedClient.sqs_queue_url, 
            MessageBody=self.large_message_body, MessageAttributes=self.message_attribute_with_queue_url)
        
        store_message_in_s3_mock.assert_called_once_with(self.large_message_body, self.message_attribute_with_queue_url)


    def test_send_message_with_small_payload(self):
        """
        Test to verify that the small message payload, with the `always_through_s3` flag turned to False, is stored in the 
        SQS queue without any modifications.

        """
        self.sqs_extended_client.always_through_s3 = False

        store_message_in_s3_mock = create_autospec(
            self.sqs_extended_client.store_message_in_s3,
            return_value=(self.small_message_body, self.small_message_attribute)
        )

        self.sqs_extended_client.store_message_in_s3 = store_message_in_s3_mock

        self.sqs_extended_client.send_message(QueueUrl=TestSQSExtendedClient.sqs_queue_url, MessageBody=self.small_message_body,
            MessageAttributes=self.small_message_attribute)
        

        store_message_in_s3_mock.assert_called_once_with(self.small_message_body, self.small_message_attribute)
    
    def test_send_message_with_small_payload_through_s3(self):
        """
        Test to verify that the small message payload, with the `always_through_s3` flag turned to True, is stored in the 
        SQS queue with the required modifications.

        """
        mocked_message_body =  _message_body_builder(False, self.s3_key,TestSQSExtendedClient.bucket_name)
        mocked_message_attribute = {}

        mocked_message_attribute[RESERVED_ATTRIBUTE_NAME] = {"DataType": "Number", "StringValue": str(len(self.small_message_body.encode("utf-8")))}


        store_message_in_s3_mock = create_autospec(
            self.sqs_extended_client.store_message_in_s3,
            return_value=(mocked_message_body, mocked_message_attribute)
        )

        self.sqs_extended_client.store_message_in_s3 = store_message_in_s3_mock

        self.sqs_extended_client.send_message(QueueUrl=TestSQSExtendedClient.sqs_queue_url, 
            MessageBody=self.small_message_body, MessageAttributes=self.message_attribute_with_queue_url)
    
        store_message_in_s3_mock.assert_called_once_with(self.small_message_body, self.message_attribute_with_queue_url)

    
    def test_retrieve_message_from_s3_invalid_message_body_format(self):
        invalid_json = """{
            "placeholder": "",
            "placeholder2": "",
            "placeholder3":[]
            }"""
        
        with pytest.raises(SQSExtendedClientException) as error:
            self.sqs_extended_client.retrieve_message_from_s3(invalid_json)
        
        self.assertEqual(str(error.value), ExceptionMessages.INVALID_FORMAT_WHEN_RETRIEVING_STORED_S3_MESSAGES)
        

    def test_retrieve_message_from_s3_small_payload_with_always_through_s3(self):

        message_body, message_attribute = self.sqs_extended_client.store_message_in_s3(self.small_message_body, self.message_attributes_with_s3_key)
        response = self.sqs_extended_client.retrieve_message_from_s3(message_body)

        self.assertEqual(self.small_message_body, response)

    def test_retrieve_message_from_s3_large_payload(self):
        
        message_body, message_attribute = self.sqs_extended_client.store_message_in_s3(self.large_message_body, self.message_attributes_with_s3_key)
        response = self.sqs_extended_client.retrieve_message_from_s3(message_body)

        self.assertEqual(self.large_message_body, response)

    
    def test_embed_s3_pointer_in_receipt_handle(self):
        """
        Note: This function, apart from testing `embed_s3_pointer_in_receipt_handle`, also goes on to 
        test `is_s3_receipt_handle` and `get_original_receipt_handle` functions.

        """
        test_message = {}
        original_receipt_handle = 'x' * 100
        mocked_message_body =  _message_body_builder(False, self.s3_key,TestSQSExtendedClient.bucket_name)
        test_message['ReceiptHandle'] = original_receipt_handle
        test_message['Body'] = mocked_message_body

        modified_receipt_handle = self.sqs_extended_client.embed_s3_pointer_in_receipt_handle(test_message)
        self.assertEqual(self.sqs_extended_client.is_s3_receipt_handle(modified_receipt_handle), True)
        self.assertEqual(self.sqs_extended_client.get_original_receipt_handle(modified_receipt_handle), original_receipt_handle)
    
    def test_receive_message_with_invalid_arguments(self):
        with pytest.raises(SQSExtendedClientException) as error:
            self.sqs_extended_client.receive_message()
        
        self.assertEqual(str(error.value), ExceptionMessages.INVALID_ARGUMENTS_FOR_RECEIVE_MESSAGE)

    def test_receive_message_with_small_payload(self):
    
        returned_message_body, _, returned_message_attributes, response_metadata = \
            self._send_and_receive_message(self.small_message_body, self.message_attributes_with_s3_key)

        self.assertEqual(self.small_message_body, returned_message_body)
        self.assertEqual(self.unmodified_message_attribute, returned_message_attributes)
        self.assertEqual(response_metadata['HTTPStatusCode'], 200)

    def test_receive_message_with_large_payload(self):

        returned_message_body, _, returned_message_attributes, response_metadata = \
            self._send_and_receive_message(self.large_message_body, self.message_attributes_with_s3_key)

        self.assertEqual(self.large_message_body, returned_message_body)
        self.assertEqual(self.unmodified_message_attribute, returned_message_attributes)
        self.assertEqual(response_metadata['HTTPStatusCode'], 200)

    def test_receive_message_with_no_payload(self):
        response = self.sqs_extended_client.receive_message(QueueUrl=TestSQSExtendedClient.sqs_queue_url, MaxNumberOfMessages=1)

        messages = response.get('Messages', None)
        self.assertEqual(messages, None)

    def test_receive_message_with_legacy_attributes(self):
        self.sqs_extended_client.use_legacy_attribute = True

        returned_message_body, _, returned_message_attributes, response_metadata = \
            self._send_and_receive_message(self.large_message_body, self.message_attributes_with_s3_key)

        self.assertEqual(self.large_message_body, returned_message_body)
        self.assertEqual(self.unmodified_message_attribute, returned_message_attributes)
        self.assertEqual(response_metadata['HTTPStatusCode'], 200)
    
    def test_delete_message_invalid_arguments(self):
        with pytest.raises(SQSExtendedClientException) as error:
            self.sqs_extended_client.delete_message()
        
        self.assertEqual(str(error.value), ExceptionMessages.INVALID_ARGUMENTS_FOR_DELETE_MESSAGE)

    def test_delete_message_from_s3_small_payload(self):

        _, modified_receipt_handle, _, _ = \
            self._send_and_receive_message(self.small_message_body, self.message_attributes_with_s3_key)

        delete_from_s3_response = self.sqs_extended_client.delete_message_from_s3(modified_receipt_handle)
        self.assertEqual(delete_from_s3_response['ResponseMetadata']['HTTPStatusCode'], 204)
    
    def test_delete_message_from_s3_large_payload(self):

        _, modified_receipt_handle, _, _ = \
            self._send_and_receive_message(self.large_message_body, self.message_attributes_with_s3_key)

        delete_from_s3_response = self.sqs_extended_client.delete_message_from_s3(modified_receipt_handle)
        self.assertEqual(delete_from_s3_response['ResponseMetadata']['HTTPStatusCode'], 204)
    
    def test_delete_message_small_payload(self):
        
        self.sqs_extended_client.delete_payload_from_s3 = True

        _, modified_receipt_handle, _, _ = \
            self._send_and_receive_message(self.small_message_body, self.message_attributes_with_s3_key)

        mock_return_value = {'ResponseMetadata': {'HTTPStatusCode': 204, 'HTTPHeaders': {}, 'RetryAttempts': 0}}
        delete_message_from_s3_mock = create_autospec(
            self.sqs_extended_client.delete_message_from_s3,
            return_value=mock_return_value
        )

        self.sqs_extended_client.delete_message_from_s3 = delete_message_from_s3_mock
        response = self.sqs_extended_client.delete_message(QueueUrl=TestSQSExtendedClient.sqs_queue_url, ReceiptHandle=modified_receipt_handle)

        delete_message_from_s3_mock.assert_called_once_with(modified_receipt_handle)

        status_code = response['ResponseMetadata']['HTTPStatusCode']
        self.assertEqual(status_code, 200)

        # Sanity check to see if the message is really gone
        self.assertEqual(self.sqs_extended_client.receive_message(QueueUrl=TestSQSExtendedClient.sqs_queue_url).get("Messages", {}), {})


    def test_delete_message_large_payload(self):
        
        self.sqs_extended_client.delete_payload_from_s3 = True

        _, modified_receipt_handle, _, _ = \
            self._send_and_receive_message(self.large_message_body, self.message_attributes_with_s3_key)

        mock_return_value = {'ResponseMetadata': {'HTTPStatusCode': 204, 'HTTPHeaders': {}, 'RetryAttempts': 0}}
        delete_message_from_s3_mock = create_autospec(
            self.sqs_extended_client.delete_message_from_s3,
            return_value=mock_return_value
        )
        
        self.sqs_extended_client.delete_message_from_s3 = delete_message_from_s3_mock
        response = self.sqs_extended_client.delete_message(QueueUrl=TestSQSExtendedClient.sqs_queue_url, ReceiptHandle=modified_receipt_handle)

        status_code = response['ResponseMetadata']['HTTPStatusCode']
        self.assertEqual(status_code, 200)

        delete_message_from_s3_mock.assert_called_once_with(modified_receipt_handle)

        # Sanity check to see if the message is really gone
        self.assertEqual(self.sqs_extended_client.receive_message(QueueUrl=TestSQSExtendedClient.sqs_queue_url).get("Messages", {}), {})
    
    def test_delete_message_large_payload_not_from_s3(self):

        self.sqs_extended_client.delete_payload_from_s3 = False

        _, modified_receipt_handle, _, _ = \
            self._send_and_receive_message(self.large_message_body, self.message_attributes_with_s3_key)

        mock_return_value = {'ResponseMetadata': {'HTTPStatusCode': 204, 'HTTPHeaders': {}, 'RetryAttempts': 0}}
        delete_message_from_s3_mock = create_autospec(
            self.sqs_extended_client.delete_message_from_s3,
            return_value=mock_return_value
        )

        self.sqs_extended_client.delete_message_from_s3 = delete_message_from_s3_mock
        response = self.sqs_extended_client.delete_message(QueueUrl=TestSQSExtendedClient.sqs_queue_url, ReceiptHandle=modified_receipt_handle)

        status_code = response['ResponseMetadata']['HTTPStatusCode']
        self.assertEqual(status_code, 200)

        delete_message_from_s3_mock.assert_not_called()

    def test_delete_message_large_payload_not_setting_payload_from_s3_attribute(self):

        del self.sqs_extended_client.delete_payload_from_s3

        _, modified_receipt_handle, _, _ = \
            self._send_and_receive_message(self.large_message_body, self.message_attributes_with_s3_key)

        mock_return_value = {'ResponseMetadata': {'HTTPStatusCode': 204, 'HTTPHeaders': {}, 'RetryAttempts': 0}}
        delete_message_from_s3_mock = create_autospec(
            self.sqs_extended_client.delete_message_from_s3,
            return_value=mock_return_value
        )

        self.sqs_extended_client.delete_message_from_s3 = delete_message_from_s3_mock
        response = self.sqs_extended_client.delete_message(QueueUrl=TestSQSExtendedClient.sqs_queue_url, ReceiptHandle=modified_receipt_handle)

        status_code = response['ResponseMetadata']['HTTPStatusCode']
        self.assertEqual(status_code, 200)

        delete_message_from_s3_mock.assert_not_called()

    
    def test_send_message_batch_with_invalid_arguments(self):
        with pytest.raises(SQSExtendedClientException) as error:
            self.sqs_extended_client.send_message_batch()
        
        self.assertEqual(str(error.value), ExceptionMessages.INVALID_ARGUMENTS_FOR_SEND_MESSAGE_BATCH)
    

    def test_send_message_batch_payloads(self):

        mocked_message_body =  _message_body_builder(False, self.s3_key,TestSQSExtendedClient.bucket_name)
        mocked_message_attribute = {}

        mocked_message_attribute[RESERVED_ATTRIBUTE_NAME] = {"DataType": "Number", "StringValue": str(len(self.large_message_body.encode("utf-8")))}

        store_message_in_s3_mock = create_autospec(
            self.sqs_extended_client.store_message_in_s3,
            return_value=(mocked_message_body, mocked_message_attribute)
        )

        self.sqs_extended_client.store_message_in_s3 = store_message_in_s3_mock

        # Testing with entries consisting of only small message payloads
        response = self.sqs_extended_client.send_message_batch(
            QueueUrl=TestSQSExtendedClient.sqs_queue_url,
            Entries=self.small_batch_entries
        )

        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertEqual(len(response['Successful']), 2)
        self.assertEqual(store_message_in_s3_mock.call_count, 2)

        store_message_in_s3_mock.reset_mock()

        # Testing with entries consisting of large message payloads too
        response = self.sqs_extended_client.send_message_batch(
            QueueUrl=TestSQSExtendedClient.sqs_queue_url,
            Entries=self.large_batch_entries
        )

        self.assertEqual(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertEqual(store_message_in_s3_mock.call_count, 2)
        self.assertEqual(store_message_in_s3_mock.call_count, 2)
    
    def test_delete_message_batch_with_invalid_arguments(self):
        with pytest.raises(SQSExtendedClientException) as error:
            self.sqs_extended_client.delete_message_batch()
        
        self.assertEqual(str(error.value), ExceptionMessages.INVALID_ARGUMENTS_FOR_DELETE_MESSAGE_BATCH)
    

    def test_delete_small_message_batch_payloads_not_through_s3(self):

        self.sqs_extended_client.delete_payload_from_s3 = False
        self.sqs_extended_client.always_through_s3 = False

        messages = self._send_and_receive_batch_messages(copy.deepcopy(self.small_batch_entries))
        delete_message_entries = self._build_batch_entries(messages, copy.deepcopy(self.small_batch_entries))

        delete_message_batch_response = self.sqs_extended_client.delete_message_batch(
            QueueUrl=TestSQSExtendedClient.sqs_queue_url,
            Entries=delete_message_entries
        )

        self.assertEqual(delete_message_batch_response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertEqual(len(delete_message_batch_response['Successful']), len(self.small_batch_entries))

    
    def test_delete_small_message_batch_payloads_through_s3_delete_from_s3(self):
        self.sqs_extended_client.delete_payload_from_s3 = True
        self.sqs_extended_client.always_through_s3 = True
    
        mock_return_value = {'ResponseMetadata': {'HTTPStatusCode': 204, 'HTTPHeaders': {}, 'RetryAttempts': 0}}
        delete_message_from_s3_mock = create_autospec(
            self.sqs_extended_client.delete_message_from_s3,
            return_value=mock_return_value
        )

        self.sqs_extended_client.delete_message_from_s3 = delete_message_from_s3_mock

        messages = self._send_and_receive_batch_messages(copy.deepcopy(self.small_batch_entries))
        delete_message_entries = self._build_batch_entries(messages, copy.deepcopy(self.small_batch_entries))

        delete_message_batch_response = self.sqs_extended_client.delete_message_batch(
            QueueUrl=TestSQSExtendedClient.sqs_queue_url,
            Entries=copy.deepcopy(delete_message_entries)
        )

        self.assertEqual(delete_message_batch_response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertEqual(len(delete_message_batch_response['Successful']), len(self.small_batch_entries))
        self.assertEqual(delete_message_from_s3_mock.call_count, 2)
    

    def test_delete_large_message_batch_payloads_through_s3_delete_from_s3(self):
        self.sqs_extended_client.delete_payload_from_s3 = True

        mock_return_value = {'ResponseMetadata': {'HTTPStatusCode': 204, 'HTTPHeaders': {}, 'RetryAttempts': 0}}
        delete_message_from_s3_mock = create_autospec(
            self.sqs_extended_client.delete_message_from_s3,
            return_value=mock_return_value
        )

        self.sqs_extended_client.delete_message_from_s3 = delete_message_from_s3_mock

        messages = self._send_and_receive_batch_messages(copy.deepcopy(self.large_batch_entries))
        delete_message_entries = self._build_batch_entries(messages, copy.deepcopy(self.large_batch_entries))

        delete_message_batch_response = self.sqs_extended_client.delete_message_batch(
            QueueUrl=TestSQSExtendedClient.sqs_queue_url,
            Entries=copy.deepcopy(delete_message_entries)
        )

        self.assertEqual(delete_message_batch_response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertEqual(len(delete_message_batch_response['Successful']), len(self.large_batch_entries))
        self.assertEqual(delete_message_from_s3_mock.call_count, 2)

    def test_delete_large_message_batch_payloads_through_s3_not_delete_from_s3(self):
        self.sqs_extended_client.delete_payload_from_s3 = False

        mock_return_value = {'ResponseMetadata': {'HTTPStatusCode': 204, 'HTTPHeaders': {}, 'RetryAttempts': 0}}
        delete_message_from_s3_mock = create_autospec(
            self.sqs_extended_client.delete_message_from_s3,
            return_value=mock_return_value
        )

        self.sqs_extended_client.delete_message_from_s3 = delete_message_from_s3_mock

        messages = self._send_and_receive_batch_messages(copy.deepcopy(self.large_batch_entries))
        delete_message_entries = self._build_batch_entries(messages, copy.deepcopy(self.large_batch_entries))

        delete_message_batch_response = self.sqs_extended_client.delete_message_batch(
            QueueUrl=TestSQSExtendedClient.sqs_queue_url,
            Entries=copy.deepcopy(delete_message_entries)
        )

        self.assertEqual(delete_message_batch_response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.assertEqual(len(delete_message_batch_response['Successful']), len(self.large_batch_entries))
        delete_message_from_s3_mock.assert_not_called()
    
    def test_change_message_visibility_with_invalid_arguments(self):
        with pytest.raises(Exception) as error:
            self.sqs_extended_client.change_message_visibility()
        
        self.assertEqual(str(error.value), ExceptionMessages.INVALID_ARGUMENTS_FOR_CHANGE_MESSAGE_VISIBILITY)
    
    def test_change_message_visibility_with_normal_receipt_handle(self):

        new_visibility_timeout = 10

        self._change_message_visibility_helper(
            self.sqs_extended_client, 
            new_visibility_timeout,
            self.small_message_body,
            self.small_message_attribute.copy()
        )

    def test_change_message_visibility_with_modified_receipt_handle(self):

        new_visibility_timeout = 10

        self._change_message_visibility_helper(
            self.sqs_extended_client, 
            new_visibility_timeout,
            self.large_message_body,
            self.small_message_attribute.copy()
        )
    
    def test_change_message_visibility_batch_with_invalid_arguments(self):
        with pytest.raises(Exception) as error:
            self.sqs_extended_client.change_message_visibility_batch()
        
        self.assertEqual(str(error.value), ExceptionMessages.INVALID_ARGUMENTS_FOR_CHANGE_MESSAGE_VISIBILITY_BATCH)


    def test_change_message_visibility_batch_with_normal_receipt_handles(self):
        self.sqs_extended_client.always_through_s3 = False
        visibility_timeout = 10

        self._change_message_visibility_batch_helper(
            self.sqs_extended_client, 
            copy.deepcopy(self.small_batch_entries),
            visibility_timeout
        )
        

    def test_change_message_visibility_batch_with_modified_receipt_handles(self):
        self.sqs_extended_client.always_through_s3 = True
        visibility_timeout = 10

        self._change_message_visibility_batch_helper(
            self.sqs_extended_client, 
            copy.deepcopy(self.large_batch_entries),
            visibility_timeout
        )
    
    def test_purge_queue_with_invalid_arguments(self):
        with pytest.raises(Exception) as error:
            self.sqs_extended_client.purge_queue()
        
        self.assertEqual(str(error.value), ExceptionMessages.INVALID_ARGUMENTS_FOR_PURGE_QUEUE)


if __name__ == "__main__":
    unittest.main()
