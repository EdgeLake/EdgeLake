import subprocess
from typing import Optional, Dict, Any, List


class AkaveConnector:
    """
    Python wrapper around akavecli for bucket and file operations.
    """

    def __init__(self, node_address: str, private_key: str):
        """
        :param node_address: e.g. "connect.akave.ai:5500"
        :param private_key_path: path to private key file (string)
        """
        self.node_address = node_address
        self.private_key = private_key

    def _run(self, args: List[str], encryption_key=None) -> str:
        """
        Internal helper to run akavecli commands.
        """
        cmd = [
            "akavecli",
            "ipc",
            *args,
            "--node-address", self.node_address,
            "--private-key", self.private_key
        ]
        if encryption_key:
            cmd.extend(["--encryption-key", encryption_key])
        try:
            result = subprocess.run(
                " ".join(cmd),
                shell=True,
                check=True,
                capture_output=True,
                text=True
            )
            return (result.stdout.strip() or result.stderr.strip())
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Command failed: {e.stderr.strip()}") from e

    # ------------------------------
    # Bucket management
    # ------------------------------

    def create_bucket(self, bucket_name: str) -> str:
        return self._run(["bucket", "create", bucket_name])

    def view_bucket(self, bucket_name: str) -> str:
        return self._run(["bucket", "view", bucket_name])

    def list_buckets(self) -> str:
        return self._run(["bucket", "list"])

    def delete_bucket(self, bucket_name: str) -> str:
        return self._run(["bucket", "delete", bucket_name])

    # ------------------------------
    # File operations
    # ------------------------------

    def list_files(self, bucket_name: str) -> str:
        return self._run(["file", "list", bucket_name])

    def file_info(self, bucket_name: str, file_name: str) -> str:
        return self._run(["file", "info", bucket_name, file_name])

    def upload_file(self, bucket_name: str, file_path: str, encryption_key=None) -> str:
        return self._run(["file", "upload", bucket_name, file_path], encryption_key)

    def download_file(self, bucket_name: str, file_name: str, dest_folder: str, encryption_key=None) -> str:
        return self._run(["file", "download", bucket_name, file_name, dest_folder], encryption_key)

    def delete_file(self, bucket_name: str, file_name: str) -> str:
        return self._run(["file", "delete", bucket_name, file_name])

def main():
    with open("/Users/roy/Github-Repos/akavesdk/.demo_private_key.key", "r") as f:
        private_key = f.read()
    # akave = AkaveConnector(node_address="connect.akave.ai:5500", private_key=private_key)
    # # print(akave.create_bucket("anylog-test/"))
    # print(akave.list_buckets())
    # # print(akave.view_bucket("anylog-test"))
    # print(akave.upload_file("anylog-test", "/Users/roy/hello.txt"))
    # # print(akave.download_file("anylog-test", "hello.txt", "/Users/roy/test"))



if __name__ == '__main__':
    main()