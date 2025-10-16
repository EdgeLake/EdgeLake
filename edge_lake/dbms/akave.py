
import sys

import edge_lake.generic.process_status as process_status
import os

try:
    lib_name_ = "boto3"
    import boto3
    from botocore.config import Config

    lib_name_ = "botocore/exceptions/ClientError"
    from botocore.exceptions import ClientError
except:
    akave_installed_ = False
else:
    akave_installed_ = True

def test_lib_installed(status):
    if akave_installed_:
        ret_val = process_status.SUCCESS
    else:
        status.add_error(f"Lib {lib_name_} not installed")
        ret_val = process_status.Failed_to_import_lib
    return ret_val
class AkaveConnector:
    """
    Python wrapper around akavecli for bucket and file operations.
    """
    def __init__(self, endpoint_url: str="https://o3-rc3.akave.xyz", region: str="akave-network", access_key: str=None, secret_key: str=None):
        """
        :param endpoint_url: Node endpoint, e.g. "https://o3-rc3.akave.xyz"
        :param region: Akave region, e.g. "akave-network"
        :access_key: AKAVE access key
        :secret_key: AKAVE secret key
        """

        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            endpoint_url=endpoint_url,
            config = Config(request_checksum_calculation="when_required", response_checksum_validation ="when_required")
        )

        self.region = region

    # ------------------------------
    # Bucket management: Create a bucket in Akave Cloud
    # ------------------------------
    def create_bucket(self, status, bucket_name: str) -> tuple:
        """
        :param status: display AnyLog status messages
        :bucket_name: Akave bucket name to be created
        """
        try:
            self.s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': self.region
                }
            )
            reply = None
        except:
            errno, value = sys.exc_info()[:2]
            reply = f"Failed to create Akave bucket ({bucket_name}):  {errno} : {value}"
            status.add_error(reply)
            ret_val = process_status.Failed_bucket_create
        else:
            ret_val = process_status.SUCCESS

        return ret_val, reply

    # ------------------------------
    # View bucket: Not supported in Akave, but may be support with AWS integration
    # ------------------------------
    def view_bucket(self, bucket_name: str) -> str:
        # return self._run(["bucket", "view", bucket_name])
        pass


    # ------------------------------
    # List all buckets in Akave
    # ------------------------------
    def list_buckets(self, status) -> tuple:
        """
        :param status: display AnyLog status messages
        """
        try:
            resp = self.s3.list_buckets()
            reply = [b["Name"] for b in resp.get("Buckets", [])]
        except:
            errno, value = sys.exc_info()[:2]
            reply = f"Failed to list Akave buckets:  {errno} : {value}"
            status.add_error(reply)
            ret_val = process_status.Failed_bucket_list
        else:
            ret_val = process_status.SUCCESS
        return ret_val, reply

    # ------------------------------
    # Delete bucket
    # ------------------------------
    def delete_bucket(self, status, bucket_name: str, delete_all: bool) -> tuple:
        """
        :param status: display AnyLog status messages
        :param bucket_name: Akave bucket name to be deleted
        :param delete_all: Delete all files in Akave bucket (a non-empty bucket cannot be deleted)
        """
        reply = None
        try:
            if delete_all:
                ret_val, file_list = self.list_files(status, bucket_name)
                if not ret_val:
                    for key in file_list:
                        ret_val, reply = self.delete_file(status, bucket_name, key)
                        if ret_val:
                            break
            self.s3.delete_bucket(Bucket=bucket_name)
        except:
            errno, value = sys.exc_info()[:2]
            reply = f"Failed to delete Akave bucket:  {errno} : {value}"
            status.add_error(reply)
            ret_val = process_status.Failed_bucket_drop
        else:
            ret_val = process_status.SUCCESS

        return ret_val, reply

      
    # ------------------------------
    # List all files in bucket
    # ------------------------------
    def list_files(self, status, bucket_name: str, prefix: str="") -> tuple:
        """
        Returns object keys under a prefix. Handles pagination.

        :param status: display AnyLog status messages
        :param bucket_name: Akave bucket name to be list files from
        :param prefix: Prefix string to be list files from
        """
        keys = []
        token = None
        try:
            while True:
                kwargs = {"Bucket": bucket_name, "Prefix": prefix}
                if token:
                    kwargs["ContinuationToken"] = token
                resp = self.s3.list_objects_v2(**kwargs)
                for item in resp.get("Contents", []):
                    keys.append(item["Key"])
                if resp.get("IsTruncated"):
                    token = resp.get("NextContinuationToken")
                else:
                    break
            reply = keys
        except:
            errno, value = sys.exc_info()[:2]
            reply = f"Failed to List Akave Bucket Files:  {errno} : {value}"
            status.add_error(reply)
            ret_val = process_status.Failed_to_list_files
        else:
            ret_val = process_status.SUCCESS

        return ret_val, reply

    # ------------------------------
    # Display file metadata
    # ------------------------------
    def file_info(self, status, bucket_name: str, file_name: str) -> tuple:
        """
        Returns metadata as a string

        :param status: display AnyLog status messages
        :param bucket_name: Akave bucket name to be list files from
        :param file_name: Akave file name to extract metadata from
        """
        try:
            obj = self.s3.head_object(Bucket=bucket_name, Key=file_name)
            reply = str(obj)
        except:
            errno, value = sys.exc_info()[:2]
            reply = f"Failed to Get Akave File Info:  {errno} : {value}"
            status.add_error(reply)
            ret_val = process_status.Failed_to_extract_file_components
        else:
            ret_val = process_status.SUCCESS

        return ret_val, reply

    # ------------------------------
    # Upload file to Akave
    # ------------------------------
    def upload_file(self, status, bucket_name: str, file_path: str, key: str, encryption_key=None) -> tuple:
        """
        :param status: display AnyLog status messages
        :param bucket_name: Akave bucket name to upload file to
        :param file_path: Akave file path
        :param key: Akave file key (this key will be used to retrieve the file)
        :param encryption_key: Akave encryption key (Need to communicate with Akave team, but I believe files are encrypted using provided secret key)
        """
        try:
            with open(file_path, "rb") as f:
                self.s3.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=f,
                    ContentType="application/octet-stream"
                )
            reply = None
        except:
            errno, value = sys.exc_info()[:2]
            reply = f"Failed to Upload File to Akave:  {errno} : {value}"
            status.add_error(reply)
            ret_val = process_status.Failed_file_upload
        else:
            ret_val = process_status.SUCCESS

        return ret_val, reply

    # ------------------------------
    # Download file
    # ------------------------------
    def download_file(self, status, bucket_name: str, key: str, file_name: str, dest_folder: str, encryption_key=None) -> tuple:
        """
        :param status: display AnyLog status messages
        :param bucket_name: Akave bucket name to download file from
        :param key: Akave file unique file key
        :param file_name: Filename to save file into dest_folder
        :param dest_folder: Destination folder to save file into
        """
        try:
            dest_write_folder = os.path.join(dest_folder, file_name)
            self.s3.download_file(bucket_name, key, dest_write_folder)
            reply = None
        except:
            errno, value = sys.exc_info()[:2]
            reply = f"Failed to Download File From Akave:  {errno} : {value}"
            status.add_error(reply)
            ret_val = process_status.Failed_file_download
        else:
            ret_val = process_status.SUCCESS

        return ret_val, reply


    # ------------------------------
    # Delete file
    # ------------------------------
    def delete_file(self, status, bucket_name: str, key: str) -> tuple:
        """
        :param status: display AnyLog status messages
        :param bucket_name: Akave bucket name to delete file from
        :param key: Akave unique file key
        """
        token = None
        try:
            self.s3.delete_object(Bucket=bucket_name, Key=key)
            reply = None
        except:
            errno, value = sys.exc_info()[:2]
            reply = f"Failed to Delete File From Akave:  {errno} : {value}"
            status.add_error(reply)
            ret_val = process_status.Failed_to_delete_file
        else:
            ret_val = process_status.SUCCESS

        return ret_val, reply

      
    # ------------------------------
    # Delete file by Prefix
    # ------------------------------
    def delete_file_by_prefix(self, status, bucket_name: str, prefix: str="") -> tuple:
        """
        Bulk-delete up to 1000 objects per request under a prefix.

        :param status: display AnyLog status messages
        :param bucket_name: Akave bucket name to delete file from
        :param prefix: Prefix string to be list files from
        """
        token = None
        try:
            files = self.list_files(status, bucket_name, prefix)
            for key in files:
                self.delete_file(status, bucket_name, key)
            reply = None
        except:
            errno, value = sys.exc_info()[:2]
            reply = f"Failed to Delete By Prefix File From Akave:  {errno} : {value}"
            status.add_error(reply)
            ret_val = process_status.Failed_to_delete_file
        else:
            ret_val = process_status.SUCCESS

        return ret_val, reply
