import boto3
import os
import json
import time
from datetime import datetime
from botocore.exceptions import ClientError
import route53_utils

bucket_name = os.environ['BUCKET']
s3 = boto3.client('s3')
route53 = boto3.client('route53')


def restore_hosted_zone(zone_to_restore):
    if zone_to_restore['Config']['PrivateZone']:
        restored_zone = route53.create_hosted_zone(
            Name=zone_to_restore['Name'],
            CallerReference=get_unique_caller_id(zone_to_restore['Id']),
            HostedZoneConfig=zone_to_restore['Config'],
            VPC=zone_to_restore['VPCs'][0]
        )['HostedZone']
    else:
        restored_zone = route53.create_hosted_zone(
            Name=zone_to_restore['Name'],
            CallerReference=get_unique_caller_id(zone_to_restore['Id']),
            HostedZoneConfig=zone_to_restore['Config']
        )['HostedZone']

    print(f"Restored the zone {zone_to_restore['Id']}")
    return restored_zone


def get_unique_caller_id(resource_id):
    """
    Creates a unique caller ID, which is required to avoid processing a single request multiple times by mistake
    :param resource_id: The ID of the resource to be restored
    :return: A unique string
    """
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", datetime.utcnow().utctimetuple())
    return f'{timestamp}-{resource_id}'


def create_zone_if_not_exist(zone_obj):
    try:
        return route53.get_hosted_zone(Id=zone_obj['Id'])['HostedZone']
    except ClientError as e:
        if e.response['Error'].get('Code', False) and e.response['Error']['Code'] == 'NoSuchHostedZone':
            return restore_hosted_zone(zone_obj)
        else:
            print(e)


def get_s3_object_as_string(key):
    return s3.get_object(Bucket=bucket_name, Key=key)['Body'].read()


def handle(event, context):
    backup_time = (
        event['BackupTime']
        if event.get('BackupTime', False)
        else get_s3_object_as_string('latest_backup_timestamp').decode()
    )

    print(f'Restoring from backup taken at {backup_time}')

    zones = json.loads(get_s3_object_as_string(f'{backup_time}/zones.json'))
    for zone_obj in zones:
        zone = create_zone_if_not_exist(zone_obj)
        backup_zone_records = json.loads(
            get_s3_object_as_string(f"{backup_time}/{zone_obj['Name']}.json")
        )

        current_zone_records = route53_utils.get_route53_zone_records(zone['Id'])

        records_to_upsert = list(filter(lambda x: x not in current_zone_records, backup_zone_records))
        if changes_list := list(
            map(
                lambda x: {"Action": "UPSERT", "ResourceRecordSet": x},
                records_to_upsert,
            )
        ):
            route53.change_resource_record_sets(
                HostedZoneId=zone['Id'],
                ChangeBatch={'Comment': 'Restored by HowCrew\'s route53 backup module', 'Changes': changes_list}
            )

    backup_health_checks = json.loads(
        get_s3_object_as_string(f'{backup_time}/Health checks.json')
    )

    current_health_checks = route53_utils.get_route53_health_checks()

    # Compare the health checks by their IDs, actual objects are a little different
    health_checks_to_create = list(filter(lambda x: x['Id'] not in list(map(lambda y: y['Id'], current_health_checks)), backup_health_checks))
    for health_check_to_create in health_checks_to_create:
        unique_caller_reference = get_unique_caller_id(health_check_to_create['Id'])
        created = route53.create_health_check(
            CallerReference=unique_caller_reference,
            HealthCheckConfig=health_check_to_create['HealthCheckConfig']
        )['HealthCheck']

        if len(health_check_to_create.get('Tags', [])) > 0:
            route53.change_tags_for_resource(ResourceType='healthcheck', ResourceId=created['Id'], AddTags=health_check_to_create['Tags'])
    return f'Restored backup from {backup_time}'
