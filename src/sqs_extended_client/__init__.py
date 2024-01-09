# Developing this file here to replace a boto3 Session with SQSExtendedClientSession

import boto3

from .client import SQSExtendedClientSession

setattr(boto3.session, "Session", SQSExtendedClientSession)

# Now take care of the reference in the boto3.__init__ module since the object is being imported there too
setattr(boto3, "Session", SQSExtendedClientSession)

# Now ensure that even the default session is our SQSExtendedClientSession
if boto3.DEFAULT_SESSION:
    boto3.setup_default_session()
