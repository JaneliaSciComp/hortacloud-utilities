''' Library of common AWS S3 functions
'''

import datetime
import boto3
from botocore.exceptions import ClientError

# *****************************************************************************
# * Internal routines                                                         *
# *****************************************************************************

def _cloudwatch(region):
    ''' Return a Cloudwatch accessor for a region
        Keyword arguments:
          region: AWS region
        Returns:
          Cloudwatch accessor
    '''
    return boto3.client('cloudwatch', region_name=region)


def _bucketstats(name, region, metric):
    ''' Get the bucket size and object count for a given date
        Keyword arguments:
          name: bucket name
          region: AWS region
          metric: metric
        Returns:
          Results dict
    '''
    metriclist = [('BucketSizeBytes', 'StandardStorage'),
                  ('NumberOfObjects', 'AllStorageTypes')]
    results = {}
    midnight = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    for metric_name, storage_type in metriclist:
        metrics = _cloudwatch(region).get_metric_statistics(
            Namespace='AWS/S3',
            MetricName=metric_name,
            StartTime=midnight - datetime.timedelta(days=1),
            EndTime=midnight,
            Period=86400,
            Statistics=[metric],
            Dimensions=[{'Name': 'BucketName', 'Value': name},
                        {'Name': 'StorageType', 'Value': storage_type}])
        if metrics['Datapoints']:
            results[metric_name] = sorted(metrics['Datapoints'],
                                          key=lambda row: row['Timestamp'])[-1][metric]
            continue
    return results


# *****************************************************************************
# * Callable routines                                                         *
# *****************************************************************************

def bucket_stats(bucket="", metric="Maximum", profile="", region="us-east-1"):
    ''' Get statistics for one or more buckets
        Keyword arguments:
          bucket: bucket name
          metric: metric ([Maximum], Minimum, Average, ExtendedStatistics,  SampleCount , Sum)
          profile: profile
          region: AWS region
        Returns:
          Rsults dict (keyed by bucket name if no bucket is specified)
    '''
    if profile:
        profiles = boto3.session.Session().available_profiles
        if profile not in profiles:
            raise ValueError("Invalid profile %s" % (profile))
        boto3.setup_default_session(profile_name=profile)
    s3r = boto3.resource('s3')
    if bucket:
        results = _bucketstats(bucket, region, metric)
        bkt = s3r.Bucket(bucket)
        stats = {"size": int(results.get('BucketSizeBytes', 0)),
                 "objects": int(results.get('NumberOfObjects', 0))}
    else:
        stats = {}
        for bkt in s3r.buckets.all():
            results = _bucketstats(bkt.name, region, metric)
            stats[bkt.name] = {"size": int(results.get('BucketSizeBytes', 0)),
                               "objects": int(results.get('NumberOfObjects', 0))}
    if profile:
        boto3.setup_default_session(profile_name="default")
    return stats


def prefix_stats(bucket, prefix=""):
    ''' Return  stats for a bucket and optional prefix
        Keyword arguments:
          bucket: bucket name
          prefix: prefix
        Returns:
          Dictionary of stats
    '''
    s3r = boto3.resource('s3')
    s3b = s3r.Bucket(bucket)
    size = objects = 0
    for object_summary in s3b.objects.filter(Prefix=prefix):
        objects += 1
        size += object_summary.size
    return {"size": size,
            "objects": objects}


def get_buckets(profile="default"):
    ''' Get a list of buckets
        Keyword arguments:
          bucket: bucket name
          profile: profile [default]
        Returns:
          List of buckets
    '''
    profiles = boto3.session.Session().available_profiles
    if profile not in profiles:
        raise ValueError("Invalid profile %s" % (profile))
    session = boto3.session.Session(profile_name=profile)
    s3c = session.client('s3')
    buckets = []
    try:
        response = s3c.list_buckets()
        for bucket in response['Buckets']:
            buckets.append(bucket["Name"])
    except ClientError:
        print("Couldn't get buckets")
        raise
    return buckets


def get_objects(bucket, prefix="", full=False):
    ''' Return a list of object keys in a bucket and optional prefix
        Keyword arguments:
          bucket: bucket name
          prefix: prefix
          full: include size
        Returns:
          List of object keys or list of object key dicts
    '''
    s3r = boto3.resource('s3')
    s3b = s3r.Bucket(bucket)
    objectlist = []
    for object_summary in s3b.objects.filter(Prefix=prefix):
        if full:
            itm = {"object": object_summary.key, "size": object_summary.size}
        else:
            itm = object_summary.key
        objectlist.append(itm)
    return objectlist


def get_prefixes(bucket, prefix="", full=False):
    ''' Return a list ob prefixes in a bucket and optional prefix
        Keyword arguments:
          bucket: bucket name
          prefix: prefix
        Returns:
          List of prefixes
    '''
    s3c = boto3.client('s3')
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    paginator = s3c.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/")
    prefixlist = []
    prefixdict = {}
    for page in pages:
        prefixes = page.get("CommonPrefixes")
        if not prefixes:
            continue
        for pfx in prefixes:
            if full:
                pre = pfx.get('Prefix').split("/")[-2]
                stats = prefix_stats(bucket, prefix + pre)
                prefixdict[pre] = stats
            else:
                prefixlist.append(pfx.get('Prefix').split("/")[-2])
    return prefixdict if full else prefixlist
