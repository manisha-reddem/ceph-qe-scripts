"""
test_bucket_notification - Test bucket notifications
Usage: test_bucket_notification.py -c <input_yaml>
<input_yaml>
    YAML should specify:
    - number of buckets: bucket_count
    - number of topics: num_topics
    - event types: events
    - object sizes: mapped_sizes
    - enable copy, delete, multipart upload, etc.
Operation:
    - create users (tenant/non-tenant)
    - create topics
    - attach bucket notifications
    - upload, copy, delete objects
    - verify events
    - delete topics and buckets
"""

import os
import sys
import json
import argparse
import uuid
import logging
import time
import traceback
from collections import defaultdict

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../..")))

import v2.lib.resource_op as s3lib
import v2.utils.utils as utils
from v2.lib.resource_op import Config
from v2.lib.rgw_config_opts import CephConfOp
from v2.lib.s3.auth import Auth
from v2.lib.admin import UserMgmt
from v2.tests.s3_swift import reusable
from v2.lib.s3.write_io_info import BasicIOInfoStructure, BucketIoInfo, IOInfoInitialize
from v2.tests.s3_swift.reusables import bucket_notification as notification
from v2.tests.s3_swift.reusables import rgw_accounts as accounts
from v2.tests.s3cmd import reusable as s3cmd_reusable
from v2.utils.log import configure_logging
from v2.utils.test_desc import AddTestInfo
from v2.utils.utils import RGWService
from v2.lib.exceptions import RGWBaseException, TestExecError, EventRecordDataError

log = logging.getLogger()
TEST_DATA_PATH = None

def test_exec(config, ssh_con):
    io_info_initialize = IOInfoInitialize()
    basic_io_structure = BasicIOInfoStructure()
    write_bucket_io_info = BucketIoInfo()
    io_info_initialize.initialize(basic_io_structure.initial())
    ceph_conf = CephConfOp(ssh_con)
    rgw_service = RGWService()
    ip_and_port = s3cmd_reusable.get_rgw_ip_and_port(ssh_con, config.ssl)

    # Create users
    if config.user_type is None:
        config.user_type = "non-tenanted"

    if config.user_type == "non-tenanted":
        all_users_info = s3lib.create_users(config.user_count)
    else:
        all_users_info = []
        umgmt = UserMgmt()
        for i in range(config.user_count):
            user_name = "user" + str(uuid.uuid4().hex[:16])
            tenant_name = "tenant" + str(i)
            tenant_user = umgmt.create_tenant_user(
                tenant_name=tenant_name, user_id=user_name, displayname=user_name
            )
            all_users_info.append(tenant_user)

    event_types = config.test_ops.get("event_type")
    if type(event_types) == str:
        event_types = [event_types]
    if "MultisiteReplication" in event_types:
        utils.add_service2_sdk_extras()
        other_site_rgw_ip = utils.get_rgw_ip(
            config.test_ops.get("sync_source_zone_master", True)
        )
        other_site_ssh_con = utils.connect_remote(other_site_rgw_ip)

    for each_user in all_users_info:
        auth = Auth(each_user, ssh_con, ssl=config.ssl)
        rgw_conn = auth.do_auth()
        if "MultisiteReplication" in event_types:
            other_site_auth = Auth(each_user, other_site_ssh_con, ssl=config.ssl)
            other_site_rgw_conn = other_site_auth.do_auth()

        rgw_s3_client = auth.do_auth_using_client()
        rgw_sns_conn = auth.do_auth_sns_client()
        bucket_expected_events = defaultdict(int)

        # get ceph version
        ceph_version_id, ceph_version_name = utils.get_ceph_version()
        rgw_admin_notif_commands_available = False
        ceph_version_id = ceph_version_id.split("-")
        ceph_version_id = ceph_version_id[0].split(".")
        if (float(ceph_version_id[0]) >= 19) or (
            float(ceph_version_id[0]) == 18
            and float(ceph_version_id[1]) >= 2
            and float(ceph_version_id[2]) >= 1
        ):
            rgw_admin_notif_commands_available = True

        # Create buckets
        if config.test_ops.get("create_bucket", False):
            log.info("no of buckets to create: %s" % config.bucket_count)
            buckets = []
            bucket_names = []
            for bc in range(config.bucket_count):
                bucket_name = utils.gen_bucket_name_from_userid(each_user["user_id"], rand_no=bc)
                bucket = reusable.create_bucket(bucket_name, rgw_conn, each_user, ip_and_port)
                buckets.append(bucket)
                bucket_names.append(bucket_name)

                if "MultisiteReplication" in event_types:
                    other_site_bucket = s3lib.resource_op(
                        {
                            "obj": other_site_rgw_conn,
                            "resource": "Bucket",
                            "args": [bucket_name],
                        }
                    )

                extra_topic_args = {}
                if config.user_type == "tenanted":
                    extra_topic_args = {
                        "tenant": each_user["tenant"],
                        "uid": each_user["user_id"],
                    }
            log.info(f"Created buckets: {bucket_names}")

        # Create topics
        if config.test_ops.get("create_topic", False):
            topics = []
            topic_names = []
            num_topics = config.test_ops.get("num_topics", 1)
            endpoint = config.test_ops.get("endpoint")
            ack_type = config.test_ops.get("ack_type", "all")
            persistent = config.test_ops.get("persistent_flag", False)
            security_type = config.test_ops.get("security_type", "PLAINTEXT")
            mechanism = config.test_ops.get("mechanism", None)

            for _ in range(num_topics):
                tid = uuid.uuid4().hex[:12]
                topic_name = "cephci-kafka-" + ack_type + "-ack-type-" + tid
                try:
                    notification.create_topic_from_kafka_broker(topic_name)
                except Exception:
                    log.warning(f"create_topic_from_kafka_broker skipped for {topic_name}")
                t = notification.create_topic(
                    rgw_sns_conn,
                    endpoint,
                    ack_type,
                    topic_name,
                    persistent,
                    security_type,
                    mechanism
                )
                topics.append(t)
                topic_names.append(topic_name)


        # get topic attributes
        if config.test_ops.get("get_topic_info", False):
            log.info("get topic attributes")
            for t, tname in zip(topics, topic_names):
                get_topic_info = notification.get_topic(
                    rgw_sns_conn, t, ceph_version_name
                )

                log.info("get kafka topic using rgw cli")
                get_rgw_topic = notification.rgw_admin_topic_notif_ops(
                    config, op="get", args={"topic": tname, **extra_topic_args}
                )
                if get_rgw_topic is False:
                    raise TestExecError(
                        "radosgw-admin topic get failed for kafka topic"
                    )


        # Attach notifications
        if config.test_ops.get("put_get_bucket_notification", False):
            events = ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]
            if "MultisiteReplication" in event_types:
                events.append("s3:ObjectSynced:*")
            # Attach notifications

            created_notifications = []
            bucket_topic_map = {}

            # --------------------------------------------------
            # 1. Create notifications
            # --------------------------------------------------
            for idx, bname in enumerate(bucket_names):
                notif_entries = []

                if len(topics) == len(bucket_names):
                    # 1:1 mapping
                    t = topics[idx]
                    tn = topic_names[idx]
                    notif_name = f"notification-{uuid.uuid4().hex[:8]}"

                    log.info("topic name: %s" % tn)
                    log.info("topic arn: %s" % t)

                    notification.put_bucket_notification(
                        rgw_s3_client, bname, notif_name, t, events, config
                    )

                    notif_entries.append({
                        "notification_name": notif_name,
                        "topic_name": tn,
                    })

                else:
                    # Many-to-many mapping
                    for t, tn in zip(topics, topic_names):
                        notif_name = f"notification-{uuid.uuid4().hex[:8]}"

                        notification.put_bucket_notification(
                            rgw_s3_client, bname, notif_name, t, events, config
                        )

                        notif_entries.append({
                            "notification_name": notif_name,
                            "topic_name": tn,
                        })

                bucket_topic_map[bname] = {"notifications": notif_entries}
                created_notifications.extend(notif_entries)

            # --------------------------------------------------
            # 2. Get bucket notification using S3 API
            # --------------------------------------------------
            for bname in bucket_topic_map:
                log.info(f"get bucket notification for bucket : {bname}")
                notification.get_bucket_notification(rgw_s3_client, bname)

            # --------------------------------------------------
            # 3. RGW admin validations (run once where possible)
            # --------------------------------------------------
            if rgw_admin_notif_commands_available:

                log.info("list topics using rgw admin cli")
                topics_list = notification.rgw_admin_topic_notif_ops(
                    config,
                    op="list",
                    args={**extra_topic_args},
                )

                # Version workaround
                if (
                    (float(ceph_version_id[0]) == 19 and
                    float(ceph_version_id[1]) == 2 and
                    float(ceph_version_id[2]) >= 1)
                    or (float(ceph_version_id[0]) >= 20)
                ):
                    topics_list_workaround = topics_list
                else:
                    topics_list_workaround = topics_list["topics"]

                listed_topics = {t["name"] for t in topics_list_workaround}

                # Verify topics exist
                for tn in topic_names:
                    if tn not in listed_topics:
                        raise TestExecError(
                            f"radosgw-admin topic list does not contain topic: {tn}"
                        )

                # --------------------------------------------------
                # 4. Per-bucket notification verification
                # --------------------------------------------------
                for bname, binfo in bucket_topic_map.items():

                    log.info(f"list notifications for bucket {bname}")
                    list_notif = notification.rgw_admin_topic_notif_ops(
                        config,
                        sub_command="notification",
                        op="list",
                        args={"bucket": bname, **extra_topic_args},
                    )

                    if list_notif is False:
                        raise TestExecError(
                            f"radosgw-admin notification list failed for bucket {bname}"
                        )


        # Upload objects
        if config.test_ops.get("create_object", False):
            for bucket, bucket_name in zip(buckets, bucket_names):
                for oc, size in list(config.mapped_sizes.items()):
                        config.obj_size = size
                        s3_object_name = utils.gen_s3_object_name(
                            bucket_name, oc
                        )
                        obj_name_temp = s3_object_name
                        if config.test_ops.get("Filter"):
                            s3_object_name = notification.get_affixed_obj_name(
                                config, obj_name_temp
                            )
                        log.info("s3 object name: %s" % s3_object_name)
                        s3_object_path = os.path.join(TEST_DATA_PATH, s3_object_name)
                        log.info("s3 object path: %s" % s3_object_path)
                        if "MultisiteReplication" in event_types:
                            bucket_resource = other_site_bucket
                        else:
                            bucket_resource = bucket
                        if config.test_ops.get("upload_type") == "multipart":
                            log.info("upload type: multipart")
                            reusable.upload_mutipart_object(
                                s3_object_name,
                                bucket_resource,
                                TEST_DATA_PATH,
                                config,
                                each_user,
                            )
                           
                        else:
                            log.info("upload type: normal")
                            reusable.upload_object(
                                s3_object_name,
                                bucket_resource,
                                TEST_DATA_PATH,
                                config,
                                each_user,
                            )               

        # --- Copy Objects ---
        if config.test_ops.get("copy_object", False):
            # Scenario 1: Copy objects from bucket0 -> bucket1
            log.info("Starting object copy between buckets")
            if len(buckets) > 1:
                src_bucket = buckets[0]
                tgt_bucket = buckets[1]
                src_name = bucket_names[0]
                tgt_name = bucket_names[1]

                # Enable notifications for target bucket if not already
                if tgt_name not in bucket_topic_map:
                    notif_name = f"notification-copy-{uuid.uuid4().hex[:8]}"
                    notification.put_bucket_notification(rgw_s3_client, tgt_name, notif_name, topics[0], events, config)
                    bucket_topic_map[tgt_name] = {"topic_name": topic_names[0], "notification_name": notif_name}
                    log.info(f"Created target bucket {tgt_name} notifications")

                objects_to_copy = reusable.get_object_list(src_name, rgw_s3_client)
                log.info(objects_to_copy)
                if not objects_to_copy:
                    log.info(f"No objects found in source bucket {src_name} to copy")
                else:
                    for obj in objects_to_copy:
                        rgw_s3_client.copy_object(
                            Bucket=tgt_name,
                            Key=obj,
                            CopySource={"Bucket": src_name, "Key": obj},
                        )
                        # copy to target generates create event on target bucket
                        if config.bucket_count > 2:
                            bucket_expected_events[tgt_name] += 1
                        log.info(f"Copied {obj} from {src_name} to {tgt_name}")

            # Scenario 2: Copy within same bucket to pseudo-directory (only one bucket)
            log.info("Starting object copy within same bucket to pseudo-directory")
            if len(buckets) > 1:
                intra_bucket = src_bucket
                intra_bucket_name = src_name
            else:
                intra_bucket = buckets[0]
                intra_bucket_name = bucket_names[0]

            objects_to_copy = reusable.get_object_list(intra_bucket_name, rgw_s3_client)
            log.info(objects_to_copy)
            if not objects_to_copy:
                log.info(f"No objects in bucket {intra_bucket_name} to copy within")
            else:
                for obj in objects_to_copy:
                    pseudo_dir_prefix = "pseudo_dir/"
                    copy_name = pseudo_dir_prefix + obj
                    rgw_s3_client.copy_object(
                        Bucket=intra_bucket_name,
                        Key=copy_name,
                        CopySource={"Bucket": intra_bucket_name, "Key": obj},
                    )
                    if config.bucket_count > 2:
                        bucket_expected_events[intra_bucket_name] += 1
                    log.info(f"Copied {obj} within {intra_bucket_name} to {copy_name}")

        # Delete objects
        if config.test_ops.get("delete_bucket_object", False):
            log.info("Deleting objects from buckets")
            for bucket, bucket_name in zip(buckets, bucket_names):
                objs = reusable.get_object_list(bucket_name, rgw_s3_client)
                delete_count = len(objs) if objs else 0
                reusable.delete_objects(bucket)
                bucket_expected_events[bucket_name] += delete_count              

        # --- Verify Events ---
        if config.test_ops.get("verify_events", False):

            if config.test_ops.get("copy_object", False):
                if len(buckets) > 1:
                    buckets_to_verify = {src_name, tgt_name}
                else:
                    buckets_to_verify = set(bucket_names)
            else:
                buckets_to_verify = set(bucket_topic_map.keys())

            if (
                config.test_ops.get("copy_object", False)
                or config.test_ops.get("delete_bucket_object", False)
            ):
                copy_total = sum(bucket_expected_events.values())
            else:
                copy_total = 0
               
            for bname, binfo in bucket_topic_map.items():

                if config.user_type != "non-tenanted":
                    bucket_name_for_verification = f"{each_user['tenant']}/{bname}"
                else:
                    bucket_name_for_verification = bname

                for notif in binfo["notifications"]:
                    topic_name = notif["topic_name"]
                    log.info(f"Verifying events for bucket {bucket_name_for_verification} on topic {topic_name}")

                    event_record_path = f"/tmp/event_record_{topic_name}"
                    log.info(f"Starting consumer for topic {topic_name}")

                    start_consumer = notification.start_kafka_broker_consumer(
                        topic_name, event_record_path
                    )
        
                    with open(event_record_path, "r") as fh:
                        raw_records = [json.loads(l) for l in fh if l.strip()]

                    records = []
                    for r in raw_records:
                        records.extend(r.get("Records", []))


                    log.info(f"Records for topic {topic_name} : {len(records)}")

                    if len(topics) == len(bucket_names):
                        # 1:1 mapping — verify only events for this bucket
                        actual = len([
                            r for r in records
                            if r.get("s3", {}).get("bucket", {}).get("name") == bucket_name_for_verification
                        ])
                        expected = config.objects_count

                        expected += bucket_expected_events.get(bname, 0)
                        
                        if config.test_ops.get("copy_object", False)and len(buckets) == 1:
                            expected += config.objects_count  


                    else:
                        # many-to-many mapping — each topic receives ALL events from ALL buckets
                        actual = len(records)

                        create_events = config.objects_count * config.bucket_count

                        extra_events = sum(bucket_expected_events.values())

                        if config.test_ops.get("copy_object", False) and len(buckets) == 1:
                            extra_events += config.objects_count

                        expected = create_events + extra_events 


                    log.info(f"Expected events for topic {topic_name} : {expected}")

                    log.info(
                        f"Bucket {bucket_name_for_verification} | "
                        f"Topic {topic_name} | Events {actual}"
                    )

                    if actual != expected:
                        raise TestExecError(
                            f"Event count mismatch for bucket {bucket_name_for_verification}, "
                            f"topic {topic_name}: got {actual}, expected {expected}"
                        )

            log.info("All bucket-topic event verification completed")

        if config.test_ops.get("rgw_admin_notification_rm", False):
            log.info("remove kafka topic using rgw cli")
            for bucket_name in bucket_names:
                topic_rm = notification.rgw_admin_topic_notif_ops(
                    config, sub_command="notification", op="rm", args={"bucket": bucket_name, **extra_topic_args}
                )
                if topic_rm is False:
                    raise TestExecError("kafka topic rm using rgw cli failed")
                        
        # Remove notifications from buckets
        if config.test_ops.get("put_empty_bucket_notification", False):              
            for bucket_name in bucket_names:
                notification.put_empty_bucket_notification(rgw_s3_client, bucket_name)

        # Delete topics from RGW and Kafka
        if config.test_ops.get("delete_bucket_object", False):
            for bucket, bucket_name in zip(buckets, bucket_names):
                reusable.delete_bucket(bucket)
            # Verify notifications removed using notification IDs (notif_name)
            for bname, binfo in bucket_topic_map.items():
                for entry in binfo.get("notifications", []):
                    notif_name = entry.get("notification_name")
                    log.info("verify notification get using rgw cli for notification %s" % notif_name)
                    get_notif_topic = notification.rgw_admin_topic_notif_ops(
                        config,
                        sub_command="notification",
                        op="get",
                        args={"bucket": bname, "notification-id": notif_name, **extra_topic_args},
                    )
                    log.info(f"get_notif_topic: {get_notif_topic}")
                    if get_notif_topic is not False:
                        raise TestExecError(
                            "radosgw-admin notification get is successful even after deleting the bucket"
                        )
            
            # delete rgw topic
            log.info("remove kafka topic using rgw cli")
            for tname in topic_names:
                topic_rm = notification.rgw_admin_topic_notif_ops(
                    config, op="rm", args={"topic": tname, **extra_topic_args}
                )
                if topic_rm is False:
                    raise TestExecError("kafka topic rm using rgw cli failed")

                # delete topic logs on kafka broker
                notification.del_topic_from_kafka_broker(tname)


if __name__ == "__main__":
    test_info = AddTestInfo("test bucket notification")
    test_info.started_info()

    try:
        project_dir = os.path.abspath(os.path.join(__file__, "../../.."))
        test_data_dir = "test_data"
        rgw_service = RGWService()
        TEST_DATA_PATH = os.path.join(project_dir, test_data_dir)
        if not os.path.exists(TEST_DATA_PATH):
            os.makedirs(TEST_DATA_PATH)

        parser = argparse.ArgumentParser(description="RGW S3 Automation")
        parser.add_argument("-c", dest="config", help="RGW Test yaml configuration")
        parser.add_argument("--rgw-node", dest="rgw_node", help="RGW Node", default="127.0.0.1")
        args = parser.parse_args()

        yaml_file = args.config
        rgw_node = args.rgw_node
        ssh_con = None
        if rgw_node != "127.0.0.1":
            ssh_con = utils.connect_remote(rgw_node)

        log_f_name = os.path.basename(os.path.splitext(yaml_file)[0])
        configure_logging(f_name=log_f_name, set_level="INFO")

        config = Config(yaml_file)
        config.read(ssh_con)
        if config.mapped_sizes is None:
            config.mapped_sizes = utils.make_mapped_sizes(config)

        test_exec(config, ssh_con)
        test_info.success_status("test passed")
        sys.exit(0)

    except (RGWBaseException, Exception) as e:
        log.error(e)
        log.error(traceback.format_exc())
        test_info.failed_status("test failed")
        sys.exit(1)
