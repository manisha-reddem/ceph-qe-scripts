"""
test_ldap_auth - Test to verify LDAP authentication from RGW

Usage: test_ldap_auth.py
Polarion ID - CEPH-9793

Operation:
    Base encode the ldap token from json
    Query RGW using s3cmd with the base encoded token as the access key
    Create a bucket on the LDAP user obtained.

"""


import argparse
import base64
import json
import logging
import os
import random
import sys
import traceback

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))


from v2.lib import resource_op
from v2.lib.aws import auth as aws_auth
from v2.lib.exceptions import RGWBaseException, TestExecError
from v2.lib.s3.write_io_info import BasicIOInfoStructure, IOInfoInitialize
from v2.tests.aws import reusable as aws_reusable
from v2.tests.s3_swift import reusable as s3_reusable
from v2.utils import utils
from v2.utils.log import configure_logging
from v2.utils.test_desc import AddTestInfo

log = logging.getLogger(__name__)
TEST_DATA_PATH = None


def test_exec(config, ssh_con):
    """
    Executes test based on configuration passed
    Args:
        config(object): Test configuration
    """
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    io_info_initialize.initialize(basic_io_structure.initial())
    # base64 encode json to get ldap token
    user_data = {
        "RGW_TOKEN": {
            "version": 1,
            "type": "ldap",
            "id": "ldapuser1",
            "key": "ldap*user1",
        }
    }
    user_data_str = json.dumps(user_data)
    token = base64.b64encode(user_data_str.encode("utf-8"))
    token_str = token.decode("utf-8")
    log.info(f"LDAP token is {token_str}")

    # Do a s3cmd query with token_str
    rgw_port = utils.get_radosgw_port_no(ssh_con)
    rgw_host, rgw_ip = utils.get_hostname_ip(ssh_con)
    aws_auth.install_aws()
    cmd = f"AWS_ACCESS_KEY_ID={token_str} AWS_SECRET_ACCESS_KEY=' ' /usr/local/bin/aws s3 ls --endpoint http://{rgw_ip}:{rgw_port}"
    utils.exec_shell_cmd(cmd)

    cmd = f"radosgw-admin user list"
    users = utils.exec_shell_cmd(cmd)
    if "ldapuser1" not in users:
        raise RGWBaseException("LDAP user not present in RGW user list")
    # Create a bucket on the LDAP user
    for bc in range(config.bucket_count):
        bucket_name = "ldap" + str(bc)
        cmd = f"AWS_ACCESS_KEY_ID={token_str} AWS_SECRET_ACCESS_KEY=' ' /usr/local/bin/aws s3 mb s3://{bucket_name} --endpoint http://{rgw_ip}:{rgw_port}"
        out = utils.exec_shell_cmd(cmd)
        log.info("Bucket created: " + bucket_name)
        for obj in range(config.objects_count):
            utils.exec_shell_cmd(f"fallocate -l 1K object{obj}")
            cmd = f"AWS_ACCESS_KEY_ID={token_str} AWS_SECRET_ACCESS_KEY=' ' /usr/local/bin/aws s3 cp object{obj} s3://{bucket_name}/object{obj} --endpoint http://{rgw_ip}:{rgw_port}"
            out = utils.exec_shell_cmd(cmd)
            log.info("Object created on the bucket owned by LDAP user")
        cmd = f"AWS_ACCESS_KEY_ID={token_str} AWS_SECRET_ACCESS_KEY=' ' /usr/local/bin/aws s3 ls s3://{bucket_name} --endpoint http://{rgw_ip}:{rgw_port}"
        out = utils.exec_shell_cmd(cmd)
        log.info(f"Listing bucket {bucket_name}: {out}")

    # check for any crashes during the execution
    crash_info = s3_reusable.check_for_crash()
    if crash_info:
        raise TestExecError("ceph daemon crash found!")


if __name__ == "__main__":

    test_info = AddTestInfo("Test to verify LDAP authentication from RGW")

    try:
        project_dir = os.path.abspath(os.path.join(__file__, "../../.."))
        test_data_dir = "test_data"
        TEST_DATA_PATH = os.path.join(project_dir, test_data_dir)
        log.info(f"TEST_DATA_PATH: {TEST_DATA_PATH}")
        if not os.path.exists(TEST_DATA_PATH):
            log.info("test data dir not exists, creating.. ")
            os.makedirs(TEST_DATA_PATH)
        parser = argparse.ArgumentParser(description="RGW S3 bucket creation using AWS")
        parser.add_argument("-c", dest="config", help="RGW S3 bucket stats using s3cmd")
        parser.add_argument(
            "-log_level",
            dest="log_level",
            help="Set Log Level [DEBUG, INFO, WARNING, ERROR, CRITICAL]",
            default="info",
        )
        parser.add_argument(
            "--rgw-node", dest="rgw_node", help="RGW Node", default="127.0.0.1"
        )
        args = parser.parse_args()
        yaml_file = args.config
        rgw_node = args.rgw_node
        ssh_con = None
        if rgw_node != "127.0.0.1":
            ssh_con = utils.connect_remote(rgw_node)
        log_f_name = os.path.basename(os.path.splitext(yaml_file)[0])
        configure_logging(f_name=log_f_name, set_level=args.log_level.upper())
        config = resource_op.Config(yaml_file)
        config.read()
        test_exec(config, ssh_con)
        test_info.success_status("test passed")
        sys.exit(0)

    except (RGWBaseException, Exception) as e:
        log.error(e)
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        sys.exit(1)

    finally:
        utils.cleanup_test_data_path(TEST_DATA_PATH)
