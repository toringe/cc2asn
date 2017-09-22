Creating Lambda Function
------------------------

Use the virtual environment
`workon aws`

Install third party modules
`pip3 install natsort -t .`

Pack everything
`zip -r9 sef-parser.zip *`

Upload and install to AWS
`aws lambda create-function --function-name SEF-Parser --zip-file fileb://sef-parser.zip --role <ARN of Execution Role> --handler SEF-parser.lambda_handler --runtime python3.6 --profile <config profile> --timeout 300 --memory-size 512 --description "Parsing SEF files" --tags owner="<Fname Lname>",project=CC2ASN`
