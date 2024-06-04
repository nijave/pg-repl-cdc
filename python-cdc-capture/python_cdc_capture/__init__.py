import io
import logging
import os
import time

import boto3
import psycopg2

from . import pypgoutput

PUBLICATION_NAME = "test"
SLOT_NAME = PUBLICATION_NAME

PART_SIZE = 5 * 1024 * 1024
MAX_PARTS = 4

BUCKET = "cdc"
MINIO = "http://minio-service:9000"

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

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO,
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        aws_session_token=None,
        config=boto3.session.Config(signature_version="s3v4"),
        verify=False,
    )

    try:
        s3.create_bucket(Bucket=BUCKET)
        log.info("Bucket '%s' created successfully", BUCKET)
    except (s3.exceptions.BucketAlreadyExists, s3.exceptions.BucketAlreadyOwnedByYou):
        log.info("Bucket '%s' already exists", BUCKET)

    while True:
        try:
            consume(s3=s3)
        except psycopg2.errors.ObjectInUse:
            log.warning("Replication slot in use")
            time.sleep(5)


def consume(*, s3: boto3.client):
    cdc_reader = pypgoutput.LogicalReplicationReader(
        publication_name=PUBLICATION_NAME,
        slot_name=SLOT_NAME,
    )

    table_last_lsn = {}
    upload_ids = {}
    upload_key = {}
    upload_parts = {}
    upload_start = {}
    upload_seq = {}
    buffers = {}
    stats_buffers_total = {}

    for message in cdc_reader:
        table = message.table_schema.table

        # init structures - TODO move to tracker object
        if table not in table_last_lsn:
            table_last_lsn[table] = 0

        if table not in buffers:
            # TODO move to object representing uploaded file type (json, parquet, arrow)
            buffers[table] = io.StringIO()

        if table not in upload_seq:
            upload_seq[table] = 1

        if table not in stats_buffers_total:
            stats_buffers_total[table] = 0

        stats_buffers_total[table] += buffers[table].write(message.json())
        stats_buffers_total[table] += buffers[table].write("\n")

        # TODO add time condition (probably split into separate thread)
        if buffers[table].tell() >= PART_SIZE:
            buf = buffers[table]
            log.info("Need to upload %s, buffer %d", table, buf.tell())

            if not upload_ids.get(table):
                # start multipart upload
                upload_key[table] = f"{table}/{message.lsn}.jsonl"
                upload = s3.create_multipart_upload(
                    Bucket=BUCKET,
                    Key=upload_key[table],
                )

                upload_id = upload["UploadId"]
                log.info("Started upload for %s: %s", table, upload_id)
                upload_ids[table] = upload_id  # fake upload
                upload_start[table] = time.time()

            if not upload_parts.get(table):
                upload_parts[table] = []

            # upload part
            buf.seek(0)
            part = s3.upload_part(
                Bucket=BUCKET,
                Key=upload_key[table],
                PartNumber=upload_seq[table],
                UploadId=upload_ids[table],
                Body=buf.read(),
            )
            upload_parts[table].append(
                {"PartNumber": upload_seq[table], "ETag": part["ETag"]}
            )
            upload_seq[table] += 1

            if upload_seq[table] >= MAX_PARTS:
                # upload and complete, reset uploads and buffers
                log.info("Completing upload for %s", table)
                s3.complete_multipart_upload(
                    Bucket=BUCKET,
                    Key=upload_key[table],
                    UploadId=upload_ids[table],
                    MultipartUpload={"Parts": upload_parts[table]},
                )

                upload_ids[table] = None
                upload_key[table] = None
                upload_parts[table] = None
                upload_start[table] = None
                upload_seq[table] = 1
                table_last_lsn[table] = message.lsn
                # Check if this is the oldest table (oldest table holds back replication slot)
                if min(table_last_lsn.values()) == table_last_lsn[table]:
                    log.info("Confirmed LSN %d", table_last_lsn[table])
                    cdc_reader.extractor.cur.send_feedback(
                        flush_lsn=table_last_lsn[table],
                    )

            buf.truncate(0)  # apparently faster to just create a new one
            buf.seek(0)
