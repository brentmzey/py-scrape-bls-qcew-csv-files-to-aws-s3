import os
from time import sleep
import boto3
# from .aws import config

# Initialize characteristics of this job
## Counties
fipcodes = [fips for fips in range(55001, 55143, 2)] # FIPS codes for the 72 Wisconsin counties,
fipcodes.append(55078) # Need to add in Menominee County -- 55078
## Years
years = [years for years in range(2014, 2020)]
print(years)
## Quarters
quarters = [quarters for quarters in range(1, 5)]
print(quarters)
## Record a list of AWS S3 Bucket object keys
s3_object_keys_list = []
for fips in fipcodes:
    for year in years:
        for qtr in quarters:
            s3_object_keys_list.append({"Key": f"dirty-data/{year}/{fips}_{year}_{qtr}.csv"})
s3_first_1000_object_keys_list = s3_object_keys_list[:1000:]
s3_remaining_object_keys_list = s3_object_keys_list[1000::]

def aws_session(region_name="us-east-2"):
    return boto3.session.Session(aws_access_key_id=os.getenv("BOTO3_AWS_ACCESS_KEY_ID"),
                                aws_secret_access_key=os.getenv("BOTO3_AWS_ACCESS_SECRET_KEY"),
                                region_name=region_name
    )
    

def upload_file_to_bucket(csv_file_url, year, qtr, fips, bucket_name="wi-county-economic-index-data"):
    session = aws_session()
    s3_resource = session.resource("s3")
    # file_dir, file_name = os.path.split(file_path)
    object_key = f"dirty-data/{year}/{fips}-{year}-{qtr}.csv"

    bucket = s3_resource.Bucket(bucket_name)
    bucket.put_object(
        ACL = "bucket-owner-full-control",
        Body=csv_file_url,
        Key=object_key,
        ContentType="text/csv",
    )

    s3_object_url = f"https://{bucket_name}.s3.amazonaws.com/{object_key}"
    return s3_object_url


def delete_bucket_objects(s3_object_keys_list: list, bucket_name: str ="wi-county-economic-index-data") -> dict:
    session = aws_session()
    s3_resource = session.resource("s3")
    # file_dir, file_name = os.path.split(file_path)
    

    bucket = s3_resource.Bucket(bucket_name)
    is_object_deleted = bucket.delete_objects( # should really use delete_objects()
        Bucket=bucket_name,
        Delete = {"Objects": s3_object_keys_list}
    )

    return is_object_deleted



for fips in fipcodes:
    for year in years:
        for qtr in quarters:
            try:
                csv_url = f"http://data.bls.gov/cew/data/api/{year}/{qtr}/area/{fips}.csv"
                s3_url = upload_file_to_bucket(csv_url, year, qtr, fips)
            except Exception as e:
                print(e)
                print(f"Error uploading file: {csv_url}")
            else:
                print(f"Successfully uploaded file: {s3_url}")
                sleep(3) # slow down fnc to avoid sending too many requests to BLS API in rapid succession

def successfully_deleted_obj_msg(obj_deleted_dict: dict) -> str:
    key = obj_deleted_dict.get("Key")
    version_id = obj_deleted_dict.get("VersionId")
    delete_marker = obj_deleted_dict.get("DeleteMarker")
    delete_marker_version_id = obj_deleted_dict.get("DeleteMarkerVersionId")
    if(delete_marker):
        return f"Successfully deleted object with key: {key}."
    return f"Issue deleting object with key: {key}. The AWS DeleteMarker returned as {delete_marker}"

def error_deleting_obj_msg_string(error_dict: dict) -> str:
    key = error_dict.get("Key")
    version_id = error_dict.get("VersionId")
    code = error_dict.get("Code")
    message = error_dict.get("Message")
    return f"Issue deleting object with key: {key}. Error code: {code} with Error message: '{message}'"



try:
    is_object_deleted = delete_bucket_objects(s3_first_1000_object_keys_list)
except Exception as e:
    print(e)
    print(f"Error batch deleting S3 objects in the object key list: {s3_first_1000_object_keys_list}")
else:
    if("Errors" in is_object_deleted):
        errors = is_object_deleted.get("Errors")
        errors_deleting = map(error_deleting_obj_msg_string, errors)
        for error_msg in errors_deleting:
            print(error_msg)
finally:
    deleted_objects = is_object_deleted.get("Deleted")
    delete_success_msgs = map(successfully_deleted_obj_msg, deleted_objects)
    for msg in delete_success_msgs:
        print(msg)

try:
    is_object_deleted = delete_bucket_objects(s3_remaining_object_keys_list)
except Exception as e:
    print(e)
    print(f"Error batch deleting S3 objects in the object key list: {s3_remaining_object_keys_list}")
else:
   if("Errors" in is_object_deleted):
        errors = is_object_deleted.get("Errors")
        errors_deleting = map(error_deleting_obj_msg_string, errors)
        for error_msg in errors_deleting:
            print(error_msg)
finally:
    deleted_objects = is_object_deleted.get("Deleted")
    delete_success_msgs = map(successfully_deleted_obj_msg, deleted_objects)
    for msg in delete_success_msgs:
        print(msg)