from minio import Minio
from typing import List


class MinioUtils:
    def __init__(self, minio: Minio):
        self.minio = minio

    def walk(self, bucket_name: str, prefix=None) -> List[str]:
        first_stage = self.minio.list_objects_v2(bucket_name, prefix)
        paths = []

        def recursive_walking(storage_object):
            if storage_object.is_dir:
                new_data = self.minio.list_objects_v2(bucket_name, prefix=storage_object.object_name)
                for item in new_data:
                    recursive_walking(item)
            else:
                paths.append(storage_object.object_name)

        for obj in first_stage:
            recursive_walking(obj)
        return paths

    def copy_to_new_bucket(self, bucket_name: str, source: str, new_bucket: str, new_name: str,
                           delete_after_copy: bool = False):
        paths = self.walk(bucket_name, source)
        for item in paths:
            file_name = item.replace(item.split("/")[0], new_name)
            self.minio.copy_object(new_bucket, file_name, f"{bucket_name}/{item}")
            if delete_after_copy:
                print(item)
                self.minio.remove_object(bucket_name, item)

    def copy_in_bucket(self, bucket_name: str, source: str, new_name: str, delete_after_copy: bool = False):
        paths = self.walk(bucket_name, source)
        for item in paths:
            file_name = item.replace(item.split("/")[0], new_name)
            self.minio.copy_object(bucket_name, file_name, f"{bucket_name}/{item}")
            if delete_after_copy:
                self.minio.remove_object(bucket_name, item)

    def rename(self, bucket_name: str, source: str, new_name: str, delete_after_copy: bool = False):
        paths = self.walk(bucket_name, source)
        for item in paths:
            split_new_name = new_name.split("/")
            index_in_path = len(split_new_name)
            temp_item = item
            split_source_path = temp_item.split("/")
            split_source_path[index_in_path - 1] = split_new_name[-1]
            final_new_path = "/".join(split_source_path)
            self.minio.copy_object(bucket_name, final_new_path, f"{bucket_name}/{item}")
            if delete_after_copy:
                self.minio.remove_object(bucket_name, item)
