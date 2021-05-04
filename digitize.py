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
from langs import LANGS
from util import encode_fn
from dlx import DB
from dlx.file.s3 import S3
from dlx.file import File, Identifier, FileExists, FileExistsIdentifierConflict, FileExistsLanguageConflict

s3_client = boto3.client('s3')
ssm_client = boto3.client('ssm')
#dynamodb_client = boto3.client('dynamodb')
dynamodb = boto3.resource('dynamodb')

db_connect = ssm_client.get_parameter(Name='connect-string')['Parameter']['Value']
db_client = DB.connect(db_connect)
creds = json.loads(ssm_client.get_parameter(Name='default-aws-credentials')['Parameter']['Value'])

# Connects to the undl files bucket
S3.connect(
    access_key_id=creds['aws_access_key_id'], access_key=creds['aws_secret_access_key'], bucket=creds['bucket']
)

bucket='digitization'
base_path = 'dgacm_bak'

parser = argparse.ArgumentParser(description='Run a dlx.files import process ad-hoc for items in a CSV file.')
parser.add_argument('--filename', metavar='filename', type=str)
parser.add_argument('--bucket', metavar='bucket', type=str)
parser.add_argument('--table', metavar='table', type=str)
parser.add_argument('--index', metavar='index', type=str)
parser.add_argument('--skipdb', action='store_true')

args = parser.parse_args()
right_now = str(date.today())
sys.stdout = open('logs/event-{}.log'.format(right_now), 'a+', buffering=1)

tmpdir = tempfile.mkdtemp()

with open(args.filename) as csvfile:
    this_reader = csv.reader(csvfile,delimiter='\t')
    for row in this_reader:
        print(row)
        filename = row[0]
        subfolder = row[1]
        symbol = row[2]
        lang = LANGS['E']
        if "," in row[2]:
            lang = []
            langs = row[3].split(',')
            for l in langs:
                lang.append(LANGS[l])
        else:
            try:
                lang = [LANGS[row[3]]]
            except KeyError:
                print(f"LanguageError: Unable to determine language for {filename} and {symbol}. This file won't be imported.")
                break

        ext = filename.split('.')[-1]
        encoded_filename = encode_fn(symbol, lang, ext)
        identifiers = []
        identifiers.append(Identifier('symbol',symbol))
        #key = "{}/{}/PDF/{}".format(base_path, subfolder, filename)

        print(encoded_filename)
        save_file = "{}/{}".format(tmpdir,filename)

        if args.skipdb:
            key = f"{base_path}/{subfolder}/{filename}"
            
        
        else:
            table = dynamodb.Table(args.table)
            # Use the filename to query the DigitizationIndex
            response = table.query(
                IndexName=args.index,
                KeyConditionExpression=Key('filename').eq(filename)
            )
            #print(response)
            key = ''
            for i in response['Items']:
                this_k = i['Key']
                #print(this_k)
                if "/PDF/" in this_k:
                    key = this_k
                    print("Found key: {}".format(key))

            if key == '':
                print(f"KeyError: {filename} for {symbol} did not match anything in {args.table}.")
                continue

        try:
            s3_client.download_file(args.bucket, key, save_file)
            print(save_file)
        except:
            print("NotFoundError: Unable to download file.")

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
            print("File already exists in the database. Continuing.")
            pass
        except FileNotFoundError:
            print(f"Error: {save_file} could not be found.")
            pass
        except:
            raise
        

        #print(filename, subfolder, symbol, lang)