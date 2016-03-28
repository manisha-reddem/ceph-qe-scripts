from lib.s3.rgw import RGW
import utils.log as log
import sys
from lib.s3.objects import KeyOp, PutContentsFromFile
import utils.utils as utils


def sample_test1():

    try:

        log.info('starting init of RGW instace. trying to authenticate')
        init_rgw = RGW('access_key', 'secret_key')

        bucky = init_rgw.bucket.create('rakesh')

        assert bucky['status'], "bucket creation failed"

        rakesh_bucket = bucky['bucket']

        key_op = KeyOp(rakesh_bucket)

        left_key = key_op.create('left')
        assert left_key['status'], "key creation failed"

        upload_from_file = PutContentsFromFile(left_key)

        random_file = utils.create_file('keysRansome', 50)

        uploaded_file = upload_from_file.put(random_file)
        assert uploaded_file['status'], "upload of key %s failed" % uploaded_file

        sys.exit(0)

    except AssertionError, e:
        log.error(e)
        sys.exit(1)


if __name__ == '__main__':

    sample_test1()
