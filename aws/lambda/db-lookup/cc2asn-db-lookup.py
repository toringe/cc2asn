import json
import boto3


def response(code, msg):
    return {'statusCode': code, 'body': msg}


def lambda_handler(event, context):
    param = event['pathParameters']
    if param is None:
        # Something is missing in the request
        resource = len(event['resource'].split('/'))
        reqcc = len(event['path'].split('/')[1])
        msg = "Missing country code"
        if resource == 3 and reqcc != 0:
                msg = "Missing record type"
        return response(400, msg)

    # Validate country code
    cc = param['cc'].upper()
    if (len(cc) != 2 or cc.isalpha() is False):
        return response(400, "Invalid country code")

    # Validate record type
    rec = "ASN"  # Default record type
    if 'record' in param:
        rec = param['record'].upper()
        if rec not in ['ASN', 'IPV4', 'IPV6', 'ALL']:
            return response(400, "Invalid record type")

    # Get record from S3
    try:
        s3 = boto3.resource('s3')
        dbfile = cc + '_' + rec
        obj = s3.Object(bucket_name="cc2asn-db", key=dbfile)
        data = obj.get()['Body'].read().decode('utf-8')
        return response(200, data)
    except Exception as e:
        print(e)
        return response(500, 'Failed to retrieve data')
