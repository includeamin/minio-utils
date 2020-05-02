# minio-utils


# Usage
## rename
```python
from minio import Minio
from minio_utils import MinioUtils
minio = Minio(endpoint="host",
                            access_key='access key',
                            secret_key='secret key',
                            secure=True)
utils = MinioUtils(minio)
utils.copy_in_bucket("bucket_name", "source directory", 'destination directory', delete_after_copy=False)
utils.rename("bucket_name",
       "old-name",
       'new_name', delete_after_copy=True)

```