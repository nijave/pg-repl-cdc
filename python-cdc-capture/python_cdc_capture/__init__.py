import io
import logging
import time
import uuid

import boto3
import psycopg2
from . import pypgoutput

PUBLICATION_NAME = "test"
SLOT_NAME = PUBLICATION_NAME

PART_SIZE = 4 * 1024 * 1024
MAX_PARTS = 4

# s3_target = boto3.resource('s3',
#     endpoint_url='https://<minio>:9000',
#     aws_access_key_id='<key_id>',
#     aws_secret_access_key='<access_key>',
#     aws_session_token=None,
#     config=boto3.session.Config(signature_version='s3v4'),
#     verify=False
# )

log = logging.getLogger("cdc-capture")
logging.basicConfig(level=logging.INFO)
logging.getLogger("pypgoutput.reader").setLevel(logging.WARNING)


def main():
    with psycopg2.connect() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            try:
                cur.execute(f"CREATE PUBLICATION {PUBLICATION_NAME} FOR ALL TABLES")
            except psycopg2.errors.DuplicateObject:
                log.warning("Publication already exists")
                conn.rollback()

    while True:
        try:
            consume()
        except psycopg2.errors.ObjectInUse:
            log.warning("Replication slot in use")
            time.sleep(5)


def consume():
    cdc_reader = pypgoutput.LogicalReplicationReader(
        publication_name=PUBLICATION_NAME,
        slot_name=SLOT_NAME,
    )

    table_last_lsn = {}
    upload_ids = {}
    upload_start = {}
    upload_seq = {}
    buffers = {}
    stats_buffers_total = {}

    for message in cdc_reader:
        table = message.table_schema.table

        # TODO move to tracker object
        # init structures
        if table not in table_last_lsn:
            table_last_lsn[table] = 0

        if table not in buffers:
            # TODO move to object representing uploaded file type (json, parquet, arrow)
            buffers[table] = io.StringIO()

        if table not in upload_seq:
            upload_seq[table] = 0

        if table not in stats_buffers_total:
            stats_buffers_total[table] = 0

        stats_buffers_total[table] += buffers[table].write(message.json(indent=2))
        stats_buffers_total[table] += buffers[table].write("\n")

        # TODO add time condition
        if buffers[table].tell() >= PART_SIZE:
            buf = buffers[table]
            log.info("Need to upload %s, buffer %d", table, buf.tell())

            if not upload_ids.get(table):
                # start multipart upload
                upload_id = str(uuid.uuid4())  # todo actually start upload

                log.info("Started upload for %s: %s", table, upload_id)
                upload_ids[table] = upload_id  # fake upload
                upload_start[table] = time.time()

            # upload part
            # todo boto3 .. upload_part ...
            upload_seq[table] += 1

            if upload_seq[table] >= MAX_PARTS - 1:
                # upload and complete, reset uploads and buffers
                # todo boto3 ... complete_upload ...
                log.info("Completing upload for %s", table)
                upload_ids[table] = None
                upload_start[table] = None
                upload_seq[table] = 0
                if min(table_last_lsn.values()) == table_last_lsn[table]:
                    # confirm_flush
                    log.info("Confirmed LSN %d", table_last_lsn[table])
                    # TODO need to modify pypgoutput.reader.ExtractRaw.msg_consumer
                    # so it doesn't automatically call send_feedback

            buf.truncate(0)  # apparently faster to just create a new one
            buf.seek(0)

            table_last_lsn[table] = message.lsn
