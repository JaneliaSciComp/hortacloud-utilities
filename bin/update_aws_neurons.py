''' This program will create and upload neuron data JSON files to the
    janelia-mouselight-imagery ASW S3 bucket. Data source is the NeuronBrowser
    database.
'''

import argparse
from datetime import datetime
import json
import os
import socket
import sys
import boto3
import botocore
import requests
import colorlog
from tqdm.auto import tqdm
from aws_s3_lib import get_prefixes

#pylint: disable=W0703

# Configuration
CONFIG = {'config': {'url': os.environ.get('CONFIG_SERVER_URL')}}
AWS = {}
MAP = {}
DATE = {}
MISSING_NEURON = {}
S3_CLIENT = S3_RESOURCE = ""
# General
BUCKET = "janelia-mouselight-imagery"
TEMPLATE = "An exception of type %s occurred. Arguments:\n%s"
COUNT = {"date_aws" : 0, "insert": 0, "metadata": 0}

# -----------------------------------------------------------------------------

def terminate_program(msg=None):
    """ Log an optional error to output, close files, and exit
        Keyword arguments:
          err: error message
        Returns:
           None
    """
    if msg:
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def call_responder(server, endpoint, payload='', authenticate=False):
    ''' Call a responder
        Keyword arguments:
          server: server
          endpoint: REST endpoint
          payload: payload for POST requests
          authenticate: pass along token in header
        Returns:
          JSON response
    '''
    if not CONFIG[server]['url']:
        terminate_program("No URL found for %s" % (server))
    url = CONFIG[server]['url'] + endpoint
    headers = {}
    if authenticate:
        headers["Authorization"] = "Bearer " + os.environ['NEURONBROWSER_JWT']
    try:
        if payload:
            headers['Accept'] = 'application/json'
            headers["Accept-Encoding"] = "gzip, deflate, br"
            headers["Connection"] = "keep-alive"
            headers["Content-Type"] = "application/json"
            headers["DNT"] = "1"
            headers['host'] = socket.gethostname()
            headers["Origin"] = "http://neuronbrowser.mouselight.int.janelia.org"
            req = requests.post(url, headers=headers, data=payload)
        else:
            if authenticate:
                req = requests.get(url, headers=headers)
            else:
                req = requests.get(url)
    except requests.exceptions.RequestException as err:
        terminate_program(err)
    if req.status_code == 200:
        try:
            return req.json()
        except json.JSONDecodeError as err:
            LOGGER.error("JSON decode error for %s", url)
            print("Request:\n", json.dumps(payload))
            print("Response:\n", req.text)
            sys.exit(-1)
        except Exception as err:
            LOGGER.error("Could not decode response")
            LOGGER.error(TEMPLATE, type(err).__name__, err.args)
            sys.exit(-1)
    terminate_program('Status: %s' % (str(req.status_code)))


def initialize_program():
    """ Initialize
    """
    global CONFIG, AWS, S3_CLIENT, S3_RESOURCE # pylint: disable=W0603
    data = call_responder('config', 'config/rest_services')
    CONFIG = data['config']
    data = call_responder('config', 'config/aws')
    AWS = data['config']
    if ARG.MANIFOLD == "dev":
        S3_CLIENT = boto3.client('s3')
        S3_RESOURCE = boto3.resource('s3')
    else:
        sts_client = boto3.client('sts')
        try:
            aro = sts_client.assume_role(RoleArn=AWS['role_arn'],
                                         RoleSessionName="AssumeRoleSession1")
        except Exception as err:
            LOGGER.error("Could not assume STS role")
            terminate_program(TEMPLATE % (type(err).__name__, err.args))
        credentials = aro['Credentials']
        S3_CLIENT = boto3.client('s3',
                                 aws_access_key_id=credentials['AccessKeyId'],
                                 aws_secret_access_key=credentials['SecretAccessKey'],
                                 aws_session_token=credentials['SessionToken'])
        S3_RESOURCE = boto3.resource('s3',
                                     aws_access_key_id=credentials['AccessKeyId'],
                                     aws_secret_access_key=credentials['SecretAccessKey'],
                                     aws_session_token=credentials['SessionToken'])


def get_mapping():
    ''' Get mapping of dates to neurons and populate MAP dictionary.
        Keyword arguments:
          None
        Returns:
          None
    '''
    payload = {"query":"{injections {sample {sampleDate} neurons {idString tag}}}"}
    response = call_responder("neuronbrowser", "", json.dumps(payload))
    for row in tqdm(response["data"]["injections"], desc="Injections"):
        if not (row["sample"] and row['neurons']):
            continue
        sdate = datetime.fromtimestamp(row["sample"]["sampleDate"]/1000).strftime("%Y-%m-%d")
        if sdate not in MAP:
            MAP[sdate] = {}
        for neuron in row["neurons"]:
            MAP[sdate][neuron["tag"]] = neuron["idString"]


def read_object(key):
    ''' Return the contents of a specified S3 object
        Keyword arguments:
          key: object key
        Returns:
          Contents of specified object
    '''
    try:
        obj = S3_CLIENT.get_object(Bucket=BUCKET, Key=key)
    except S3_CLIENT.exceptions.NoSuchKey as err:
        return None
    except Exception as err:
        terminate_program(TEMPLATE % (type(err).__name__, err.args))
    txt = obj['Body'].read().decode('utf-8')
    return txt


def process_prefix(tloc):
    ''' Return the contents of a specified S3 object
        Keyword arguments:
          tloc: tracings location
        Returns:
          None
    '''
    dates = get_prefixes(BUCKET, prefix="tracings/" + tloc)
    for date in tqdm(dates, desc=tloc, position=0, leave=False):
        mdata = {}
        DATE[date] = True
        if date not in MAP:
            LOGGER.warning("%s has no neuron mappings", date)
            MISSING_NEURON[date] = True
            continue
        pre = "/".join(["tracings", tloc, date])
        names = get_prefixes(BUCKET, prefix=pre)
        if not names:
            terminate_program("%s/%s has no prefixes on AWS S3" % (tloc, date))
        mdata[date] = {}
        populated = False
        for name in tqdm(names, desc="Neuron tag", position=1, leave=False):
            if name not in MAP[date]:
                #LOGGER.warning("Name %s in not in mapping for %s", name, date)
                continue
            if not name.startswith("G-"):
                continue
            populated = True
            payload = {"originalName": name}
            key = "/".join([pre, name, "soma.txt"])
            somaloc = read_object(key)
            if somaloc:
                payload["somaLocation"] = somaloc
            mdata[date][MAP[date][name]] = payload
        if populated:
            key = "/".join(["neurons", tloc, date, "metadata.json"])
            COUNT["metadata"] += 1
            if ARG.WRITE:
                # AWS S3
                try:
                    obj = S3_RESOURCE.Object(BUCKET, key)
                    result = obj.put(Body=json.dumps(mdata[date]))
                except Exception as err:
                    terminate_program(TEMPLATE % (type(err).__name__, err.args))


def process_neurons():
    ''' Process mapped neurons
        Keyword arguments:
          None
        Returns:
          None
    '''
    get_mapping()
    for tloc in ("Finished_Neurons", "tracing_complete"):
        process_prefix(tloc)
    print(f"Dates in AWS S3:         {len(DATE)}")
    print(f"Missing neuron mappings: {len(MISSING_NEURON)}")
    print(f"Metadata files written:  {COUNT['metadata']}")


# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Update neurons on AWS S3")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'], help='manifold')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False,
                        help='Flag, Actually modify image state')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()

    LOGGER = colorlog.getLogger()
    if ARG.DEBUG:
        LOGGER.setLevel(colorlog.colorlog.logging.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(colorlog.colorlog.logging.INFO)
    else:
        LOGGER.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)

    initialize_program()
    process_neurons()
    terminate_program()
