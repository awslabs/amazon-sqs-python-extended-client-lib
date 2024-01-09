from botocore.exceptions import BotoCoreError, ClientError

class SQSExtendedClientException(BotoCoreError):
    """  
    
    Base Class for all SQS Extended Client exceptions which occur
    after interacting with the SQS Extended Client.
    
    """

    def __init__(self, error_message):
        self.fmt = error_message
        super().__init__()


class SQSExtendedClientServiceException(ClientError):
    """
    Base Class for all exceptions which occur after interacting
    with services external to the SQS Extended Client, such as S3.

    """

    def __init__(self, error_response, operation_name):
        super().__init__(error_response, operation_name)