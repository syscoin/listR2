import math
import boto3
from botocore.exceptions import ClientError
import os
from collections import OrderedDict
from configparser import ConfigParser

import time

print_conf = False

def load_config():
    # Load configuration from file 'listR2_config.ini'
    config_parser = ConfigParser(dict_type=OrderedDict, inline_comment_prefixes='#')
    config = {}

    # Set some defaults first
    default = {
        'global': {'mainnet': 'false',
                   'testnet': 'false',
                   'sum_only': 'true'
        },
        'mainnet': {'first': '0',
                    'last': '0',
                    'with_size': 'false',
                    'prev_sum': '0'
        },
        'mainnet2': {'check_secondary': 'false'
        },
        'testnet': {'first': '0',
                    'last': '0',
                    'with_size': 'false',
                    'prev_sum': '0'
        },
        'testnet2': {'check_secondary': 'false'
        }
    }

    # Now get settings from file
    if config_parser.read('listR2_config.ini'):
        # Global configuration
        if config_parser.has_section('global'):
            config['global'] = dict(config_parser.items('global'))
            if 'mainnet' in config['global']:
                config['global']['mainnet'] = config['global']['mainnet'].lower() == 'true'
            if 'testnet' in config['global']:
                config['global']['testnet'] = config['global']['testnet'].lower() == 'true'
            if 'sum_only' in config['global']:
                config['global']['sum_only'] = config['global']['sum_only'].lower() == 'true'

        # Mainnet configuration
        if config_parser.has_section('mainnet'):
            config['mainnet'] = dict(config_parser.items('mainnet'))
            # Convert numeric values from strings to appropriate types
            for key in ['first', 'last', 'prev_sum']:
                if key in config['mainnet']:
                    config['mainnet'][key] = int(config['mainnet'][key])
            if 'with_size' in config['mainnet']:
                config['mainnet']['with_size'] = config['mainnet']['with_size'].lower() == 'true'

        # Mainnet2 configuration
        if config_parser.has_section('mainnet2'):
            config['mainnet2'] = dict(config_parser.items('mainnet2'))
            if 'check_secondary' in config['mainnet2']:
                config['mainnet2']['check_secondary'] = config['mainnet2']['check_secondary'].lower() == 'true'
            if 'copy_missing' in config['mainnet2']:
                config['mainnet2']['copy_missing'] = config['mainnet2']['copy_missing'].lower() == 'true'

        # Testnet configuration
        if config_parser.has_section('testnet'):
            config['testnet'] = dict(config_parser.items('testnet'))
            for key in ['first', 'last', 'prev_sum']:
                if key in config['testnet']:
                    config['testnet'][key] = int(config['testnet'][key])
            if 'with_size' in config['testnet']:
                config['testnet']['with_size'] = config['testnet']['with_size'].lower() == 'true'

        # Testnet2 configuration
        if config_parser.has_section('testnet2'):
            config['testnet2'] = dict(config_parser.items('testnet2'))
            if 'check_secondary' in config['testnet2']:
                config['testnet2']['check_secondary'] = config['testnet2']['check_secondary'].lower() == 'true'
            if 'copy_missing' in config['testnet2']:
                config['testnet2']['copy_missing'] = config['testnet2']['copy_missing'].lower() == 'true'

    if print_conf:
        # Print the configuration for verification
        print("Loaded configuration:")
        #for section_name, section_data in config.items():
        for section_name in config_parser.sections():
            print(f"[{section_name}]")
            for key in config[section_name]:
                value = config[section_name][key]
                # Mask sensitive information
                if any(sensitive in key.lower() for sensitive in ['key', 'secret', 'password', 'token']):
                    masked_value = value[:4] + '*' * (len(value) - 8) + value[-4:] if len(value) > 8 else '****'
                    print(f"  {key} = {masked_value}")
                else:
                    print(f"  {key} = {value}")
            print()

    return config


def convert_size(size_bytes):
    # Convert bytes to human readable format
    if size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    div = 1000
    i = int(math.floor(math.log(size_bytes, div)))
    s = round(size_bytes / math.pow(div, i), 2)
    return "%s %s" % (s, size_name[i])


def get_last_modified(obj):
    return obj.last_modified


def get_bucket_object_count(s3_resource, bucket_name):
    try:
        bucket = s3_resource.Bucket(bucket_name)
        count = sum(1 for _ in bucket.objects.all())
        return count
    except Exception as e:
        print(f"Error getting object count for {bucket_name}: {e}")
        return None


def get_bucket_object_count_r2(s3_resource, bucket_name):
    # Get object count for R2 buckets - handles pagination properly
    # only little faster (within 10% range); max keys per page is 1000
    try:
        paginator = s3_resource.meta.client.get_paginator('list_objects_v2')
        count = 0
        for page_num, page in enumerate(paginator.paginate(Bucket=bucket_name), 1):
            objects_in_page = len(page.get('Contents', []))
            count += objects_in_page
        return count
    except Exception as e:
        print(f"Error counting objects in R2 bucket: {e}")
        return None

def object_exists_in_bucket(s3_resource, bucket_name, object_key):
    # Check if an object exists in the specified bucket
    try:
        s3_resource.Object(bucket_name, object_key).load()
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            print(f"ERROR checking object {object_key}: {e}")
            return False
    except Exception as e:
        print(f"ERROR checking object {object_key}: {e}")
        return False


def copy_object_to_bucket(source_s3, source_bucket, source_key, dest_s3, dest_bucket, dest_key):
    # Copy object from source bucket to destination bucket
    try:
        # Get file content and type
        source_obj = source_s3.Object(source_bucket, source_key)
        file_content = source_obj.get()['Body'].read()
        content_type = source_obj.content_type
        # Upload to destination bucket
        if content_type is None:
            dest_s3.Object(dest_bucket, dest_key).put(Body=file_content)
        else:
            dest_s3.Object(dest_bucket, dest_key).put(Body=file_content, ContentType=content_type)
        return True
    except ClientError as e:
        print(f"ERROR copying {source_key}: {e}")
        return False
    except Exception as e:
        print(f"ERROR copying {source_key}: {e}")
        return False


def create_s3_resource(endpoint_url, aws_access_key_id, aws_secret_access_key):
    # Create an S3 resource from configuration
    try:
        s3_resource = boto3.resource(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        # Test the connection by listing buckets
        #buckets = [bucket.name for bucket in s3_resource.buckets.all()]
        #print(f"Successfully connected. Found buckets [{', '.join(buckets)}].")
        return s3_resource
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"S3 Client Error ({error_code}): {e.response['Error']['Message']}")
        raise
    except Exception as e:
        print(f"Unexpected error creating S3 resource: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        raise


def process_bucket(s3_resource, bucket_name, summary_only=True,
                  first=0, last=0, with_size=True, prev_sum=0,
                  check_secondary=False, copy_missing=False,
                  secondary_s3=None, secondary_bucket=None):
    # Process a bucket according to the specified parameters

    #print(f"\nProcessing bucket: {bucket_name}")

    # List all buckets
    try:
        buckets = [bucket.name for bucket in s3_resource.buckets.all()]
        print(f"Buckets: {len(buckets)}  [{', '.join(buckets)}]\n")
    except Exception as e:
        print(f"Error listing buckets: {e}")
        return

    # Get the specific bucket
    try:
        bucket = s3_resource.Bucket(bucket_name)
        bucket_objects = get_bucket_object_count(s3_resource, bucket_name)
        print(f"{bucket.name}: {bucket_objects} objects\n")
    except Exception as e:
        print(f"Error accessing bucket {bucket_name}: {e}")
        return

    # Check secondary bucket configuration if needed
    s2_configured = False
    if check_secondary and secondary_s3 and secondary_bucket:
        try:
            secondary_s3.buckets.all()  # Test connection
            bucket_2 = secondary_s3.Bucket(secondary_bucket)
            bucket_2_objects = get_bucket_object_count(secondary_s3, secondary_bucket)
            print(f"Compare to  {secondary_bucket}: {bucket_2_objects} objects\n")
            s2_configured = True
        except Exception as e:
            print(f"WARNING: Cannot connect to secondary bucket: {e}")
            check_secondary = False
            copy_missing = False

    # Exit is summary only
    if summary_only:
        return

    # Process objects
    item_sum = prev_sum
    missing_count = 0
    copied_count = 0

    try:
        # Sort objects by last modified date
        item_list = list(bucket.objects.all())
        item_list.sort(key=get_last_modified)

        # Process objects in range
        print(f"List of Objects in Bucket: {bucket.name}\n")

        for index, item in enumerate(item_list[first-1:last], start=first):
            if index > last:
                break
            item_key = item.key
            item_size = s3_resource.Object(bucket.name, item.key).content_length if with_size else 0

            # Check secondary bucket if configured
            in_secondary = ""
            object_copied = False
            if check_secondary and s2_configured:
                exists_in_secondary = object_exists_in_bucket(secondary_s3, secondary_bucket, item_key)
                if not exists_in_secondary:
                    missing_count += 1
                    in_secondary = " [NOT_IN_2]"

                    # Copy object if needed
                    if copy_missing:
                        if copy_object_to_bucket(
                            s3_resource, bucket_name, item_key,
                            secondary_s3, secondary_bucket, item_key
                        ):
                            copied_count += 1
                            object_copied = True
                            in_secondary += " [COPIED]"

            # Print object info
            print(f"{index:6d}) {item.last_modified.strftime('%Y-%m-%d %H:%M:%S')} key: {item_key}", end="")
            if with_size:
                item_sum += item_size
                print(f"  size: {item_size}{in_secondary}")
            else:
                print(in_secondary)

        # Print summary
        if with_size:
            print(f"\nTotal size of listed objects: {convert_size(item_sum)} ({item_sum})")

        if missing_count > 0:
            if copied_count > 0:
                print(f"Copied {copied_count} objects to bucket '{secondary_bucket}'")
            else:
                print(f"Missing {missing_count} objects in bucket '{secondary_bucket}'")
        elif check_secondary:
            print(f"All checked objects exist in bucket '{secondary_bucket}'")

    except Exception as e:
        print(f"Error processing objects: {e}")

    print()


def main():
    # Load configuration
    config = load_config()

    if (config['global']['mainnet'] and config['global']['testnet']) or config['global']['sum_only']:
        only_bucket_sum = True
    else:
        only_bucket_sum = False

    # Mainnet processing
    mainnet_enabled = config['global']['mainnet']
    if mainnet_enabled and 'mainnet' in config:
        print("\n>>>>> Mainnet Processing <<<<<")
        s3_mainnet = create_s3_resource(
            config['mainnet']['endpoint_url'],
            config['mainnet']['aws_access_key_id'],
            config['mainnet']['aws_secret_access_key']
        )

        if config['mainnet2']['check_secondary']:
            s3_mainnet2 = create_s3_resource(
                config['mainnet2']['endpoint_url'],
                config['mainnet2']['aws_access_key_id'],
                config['mainnet2']['aws_secret_access_key']
            )
        else:
            s3_mainnet2 = None

        process_bucket(
            s3_mainnet,
            bucket_name=config['mainnet']['bucket_name'],
            summary_only=only_bucket_sum,
            first=config['mainnet']['first'],
            last=config['mainnet']['last'],
            with_size=config['mainnet']['with_size'],
            prev_sum=config['mainnet']['prev_sum'],
            check_secondary=config['mainnet2']['check_secondary'],
            copy_missing=config['mainnet2']['copy_missing'],
            secondary_s3=s3_mainnet2,
            secondary_bucket=config['mainnet2']['bucket_name']
        )

    # Testnet processing
    testnet_enabled = config['global']['testnet']
    if testnet_enabled and 'testnet' in config and 'testnet2' in config:
        print("\n>>>>> Testnet Processing <<<<<")
        s3_testnet = create_s3_resource(
            config['testnet']['endpoint_url'],
            config['testnet']['aws_access_key_id'],
            config['testnet']['aws_secret_access_key']
        )

        if config['testnet2']['check_secondary']:
            s3_testnet2 = create_s3_resource(
                config['testnet2']['endpoint_url'],
                config['testnet2']['aws_access_key_id'],
                config['testnet2']['aws_secret_access_key']
            )
        else:
            s3_testnet2 = None

        process_bucket(
            s3_testnet,
            bucket_name=config['testnet']['bucket_name'],
            summary_only=only_bucket_sum,
            first=config['testnet']['first'],
            last=config['testnet']['last'],
            with_size=config['testnet']['with_size'],
            prev_sum=config['testnet']['prev_sum'],
            check_secondary=config['testnet2']['check_secondary'],
            copy_missing=config['testnet2']['copy_missing'],
            secondary_s3=s3_testnet2,
            secondary_bucket=config['testnet2']['bucket_name']
        )


if __name__ == "__main__":
    main()
