import boto3
import pytest
from sqs_extended_client import SQSExtendedClientSession

@pytest.fixture()
def region_name() -> str:
    region_name = 'us-east-1'
    return region_name

@pytest.fixture()
def session(region_name) -> boto3.Session:

    setattr(boto3.session, "Session", SQSExtendedClientSession)
    # Now take care of the reference in the boto3.__init__ module since the object is being imported there too
    setattr(boto3, "Session", SQSExtendedClientSession)

    # return boto3.session.Session()
    return boto3.Session(region_name=region_name)    
    
