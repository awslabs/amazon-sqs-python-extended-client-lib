class ExceptionMessages:

    INVALID_ARGUMENTS_FOR_SEND_MESSAGE = "Invalid number of arguments while calling send_message."
    
    INVALID_ARGUMENTS_FOR_RECEIVE_MESSAGE = "Invalid number of arguments while calling receive_message."

    INVALID_ARGUMENTS_FOR_DELETE_MESSAGE = "Invalid number of arguments while calling delete_message."
    
    INVALID_ARGUMENTS_FOR_CHANGE_MESSAGE_VISIBILITY = "Invalid number of arguments while calling change_message_visibility."

    INVALID_ARGUMENTS_FOR_PURGE_QUEUE = "Invalid number of arguments while calling purge_queue."

    INVALID_ARGUMENTS_FOR_CHANGE_MESSAGE_VISIBILITY_BATCH = "Invalid number of arguments while calling change_message_visibility_batch."
    
    INVALID_ARGUMENTS_FOR_SEND_MESSAGE_BATCH = "Invalid number of arguments while calling send_message_batch."

    INVALID_ARGUMENTS_FOR_DELETE_MESSAGE_BATCH = "Invalid number of arguments while calling delete_message_batch."

    INVALID_MESSAGE_ATTRIBUTE_SIZE = "Total size of message attributes is {0} bytes which is larger than the threshold of {1} bytes. Consider including the payload in the message body instead of the message attributes. "

    INVALID_NUMBER_OF_MESSAGE_ATTRIBUTES =  "Number of message attributes {0} exceeds the maximum allowed for large-payload messages {1} "

    INVALID_ATTRIBUTE_NAME_PRESENT = "Message attribute name {0} is reserved for use by the SQS extended client. "

    INVALID_MESSAGE_BODY = "messageBody cannot be null or empty."

    INVALID_FORMAT_WHEN_RETRIEVING_STORED_S3_MESSAGES = "Invalid payload format for retrieving stored messages in S3"

    FAILED_DELETE_MESSAGE_FROM_S3 = "delete_object failed with status code {0}"