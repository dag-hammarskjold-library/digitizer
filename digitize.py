import boto3
from boto3 import dynamodb
from boto3.dynamodb.conditions import Key
import argparse
import csv
import json
import re
import mimetypes
import tempfile
import sys
from datetime import date
from dlx import DB
from dlx.file.s3 import S3
from dlx.file import File, Identifier, FileExists, FileExistsIdentifierConflict, FileExistsLanguageConflict
from dlx.util import ISO6391

s3_client = boto3.client('s3')
ssm_client = boto3.client('ssm')
#dynamodb_client = boto3.client('dynamodb')
dynamodb = boto3.resource('dynamodb')

db_connect = ssm_client.get_parameter(Name='connect-string')['Parameter']['Value']
db_client = DB.connect(db_connect)
creds = json.loads(ssm_client.get_parameter(Name='default-aws-credentials')['Parameter']['Value'])

# Connects to the undl files bucket
S3.connect(
    creds['aws_access_key_id'], creds['aws_secret_access_key'], creds['bucket']
)

bucket='digitization'
base_path = 'dgacm_bak'

def encode_fn(symbols, language, extension):
    ISO6391.codes[language.lower()]
    symbols = [symbols] if isinstance(symbols, str) else symbols
    xsymbols = [sym.translate(str.maketrans(' /[]*:;', '__^^!#%')) for sym in symbols]

    return '{}-{}.{}'.format('&'.join(xsymbols), language.upper(), extension)

LANGS = {
    'A': 'AR',
    'C': 'ZH',
    'E': 'EN',
    'F': 'FR',
    'G': 'DE',
    'R': 'RU',
    'S': 'ES'
}

parser = argparse.ArgumentParser(description='Run a dlx.files import process ad-hoc for items in a CSV file.')
parser.add_argument('--filename', metavar='filename', type=str)
parser.add_argument('--bucket', metavar='bucket', type=str)
parser.add_argument('--table', metavar='table', type=str)
parser.add_argument('--index', metavar='index', type=str)

args = parser.parse_args()
right_now = str(date.today())
sys.stdout = open('logs/event-{}.log'.format(right_now), 'a+', buffering=1)

table = dynamodb.Table(args.table)

tmpdir = tempfile.mkdtemp()

with open(args.filename) as csvfile:
    this_reader = csv.reader(csvfile,delimiter='\t')
    for row in this_reader:
        filename = row[0]
        subfolder = row[1]
        symbol = row[2]
        lang = LANGS['E']
        print(f"Processing symbol {symbol}")
        if "," in row[3]:
            lang = []
            langs = row[3].split(',')
            for l in langs:
                lang.append(LANGS[l])
        else:
            try:
                lang = [LANGS[row[3]]]
            except KeyError:
                print(f"Unable to determine language for {symbol}. Defaulting to English.")
                lang = LANGS['E']

        ext = filename.split('.')[-1]
        encoded_filename = encode_fn(symbol, lang, ext)
        identifiers = []
        identifiers.append(Identifier('symbol',symbol))

        # Use the filename to query the DigitizationIndex
        try:
            response = table.query(
                IndexName=args.index,
                KeyConditionExpression=Key('filename').eq(filename)
            )

            key = ''
            for i in response['Items']:
                this_k = i['Key']
                if "/PDF/" in this_k:
                    key = this_k
                    print("Found key: {}".format(key))
        except:
            # Try to construct a key from what other data we have
            key = f"dgacm_bak/{subfolder}/PDF/{filename}"

        save_file = ''
        try:
            save_file = "{}/{}".format(tmpdir,filename)
            print(key)
            s3_client.download_file(args.bucket, key, save_file)
            print(save_file)
        except:
            print(f"Unable to find anything matching {filename} or {symbol} in {args.bucket} or {table}")

        try:
            print("Importing {}".format(encoded_filename))
            imported = File.import_from_handle(
                handle=open(save_file, 'rb'),
                filename=encoded_filename,
                identifiers=identifiers,
                languages=lang, 
                mimetype=mimetypes.guess_type(filename)[0], 
                source='adhoc::digitization'
            )
            print("Imported {}".format(imported))
        except FileExists:
            print(f"DuplicateFileWarning: File {encoded_filename} already exists in the database. Continuing.")
            pass
        except Exception as e:
            print(f"UncaughtError: {e}")