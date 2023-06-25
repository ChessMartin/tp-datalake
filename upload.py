import os
import random

from azure.storage.filedatalake import DataLakeServiceClient

SOURCE_FILE = 'SampleSource.txt'

def recursive_file_gen(mydir):
    for root, dirs, files in os.walk(mydir):
        for file in files:
            yield os.path.join(root, file)


def upload_download_sample(filesystem_client):
    file_name = "mydatalake"
    print("Creating a file named '{}'.".format(file_name))
    f = list(recursive_file_gen("data"))

    print(f)
    for i in f:
        file_client = filesystem_client.get_file_client(i)
        file_client.create_file()
        try:
            variable = open(i, 'rb')
            variable2 = variable.read()
            file_client.append_data(
                data=variable2, offset=0, length=len(variable2))
            file_client.flush_data(len(variable2))

        finally:
            pass


def get_random_bytes(size):
    rand = random.Random()
    result = bytearray(size)
    for i in range(size):
        # random() is consistent between python 2 and 3
        result[i] = int(rand.random() * 255)
    return bytes(result)


def run():
    account_name = os.getenv('STORAGE_ACCOUNT_NAME', "martindatalake")
    account_key = os.getenv(
        'STORAGE_ACCOUNT_KEY', "LiZ8Cmio7GF6PxWkf0KeXKNaKP+aTdVotJ1QUKOJ8E1iosmSZ9bUX5vxU9C6b2lOgq8xo1//ey+S+AStYTGMrA==")

    # set up the service client with the credentials from the environment variables
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https",
        account_name
    ), credential=account_key)

    fs_name = "martin-datalake"
    print("Generating a test filesystem named '{}'.".format(fs_name))

    filesystem_client = service_client.create_file_system(file_system=fs_name)

    try:
        upload_download_sample(filesystem_client)
    finally:
        pass


if __name__ == '__main__':
    run()
