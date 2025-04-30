# Copyright (c) 2025 Stephen G. Pope
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.

import os
import logging
from flask import Blueprint, request, jsonify
import threading
import requests
import uuid
import json
from datetime import datetime
import time
import psutil

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from googleapiclient.http import MediaFileUpload

from services.file_management import download_file
from services.authentication import authenticate
from app_utils import validate_payload, queue_task_wrapper

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the blueprint
gdrive_upload_bp = Blueprint('gdrive_upload', __name__)

# Environment variables
STORAGE_PATH = "/app/files/"
GCP_SA_CREDENTIALS = os.getenv('GCP_SA_CREDENTIALS')
GDRIVE_USER = os.getenv('GDRIVE_USER')

# Class to track upload progress
class UploadProgress:
    def __init__(self, job_id, total_size):
        self.job_id = job_id
        self.total_size = total_size
        self.bytes_uploaded = 0
        self.start_time = time.time()
        self.lock = threading.Lock()
        self.last_logged_percentage = 0
        self.last_logged_resource_percentage = 0

# Global list to keep track of active uploads
active_uploads = []
uploads_lock = threading.Lock()

def get_access_token():
    credentials_info = json.loads(GCP_SA_CREDENTIALS)
    credentials = Credentials.from_service_account_info(
        credentials_info,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    delegated_credentials = credentials.with_subject(GDRIVE_USER)
    if not delegated_credentials.valid or delegated_credentials.expired:
        delegated_credentials.refresh(Request())
    return delegated_credentials.token

def initiate_resumable_upload(filename, folder_id, mime_type='application/octet-stream'):
    url = 'https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable'
    headers = {
        'Authorization': f'Bearer {get_access_token()}',
        'Content-Type': 'application/json; charset=UTF-8',
        'X-Upload-Content-Type': mime_type
    }
    metadata = {
        'name': filename,
        'parents': [folder_id]
    }
    response = requests.post(url, headers=headers, data=json.dumps(metadata))
    response.raise_for_status()
    return response.headers['Location']

def upload_file_in_chunks(file_url, upload_url, total_size, job_id, chunk_size):
    bytes_uploaded = 0
    max_retries = 5
    retry_delay = 5
    progress = UploadProgress(job_id, total_size)

    with uploads_lock:
        active_uploads.append(progress)

    try:
        with requests.get(file_url, stream=True) as r:
            r.raise_for_status()
            iterator = r.iter_content(chunk_size=chunk_size)
            for chunk in iterator:
                if chunk:
                    for attempt in range(max_retries):
                        start = bytes_uploaded
                        end = bytes_uploaded + len(chunk) - 1
                        content_range = f'bytes {start}-{end}/{total_size}'
                        headers = {
                            'Content-Length': str(len(chunk)),
                            'Content-Range': content_range,
                        }
                        try:
                            upload_response = requests.put(
                                upload_url,
                                headers=headers,
                                data=chunk
                            )
                            if upload_response.status_code in (200, 201):
                                logger.info(f"Job {job_id}: Upload complete.")
                                with progress.lock:
                                    progress.bytes_uploaded = end + 1
                                return upload_response.json()['id']
                            elif upload_response.status_code == 308:
                                bytes_uploaded = end + 1
                                with progress.lock:
                                    progress.bytes_uploaded = bytes_uploaded
                                break
                            else:
                                logger.error(f"Job {job_id}: Unexpected status code: {upload_response.status_code}")
                                raise Exception(f"Upload failed with status code {upload_response.status_code}")
                        except requests.exceptions.RequestException as e:
                            logger.error(f"Job {job_id}: Network error during upload: {e}")
                            if attempt < max_retries - 1:
                                logger.info(f"Job {job_id}: Retrying upload chunk after {retry_delay} seconds...")
                                time.sleep(retry_delay)
                            else:
                                logger.error(f"Job {job_id}: Max retries reached. Upload failed.")
                                raise
    finally:
        with uploads_lock:
            if progress in active_uploads:
                active_uploads.remove(progress)

@gdrive_upload_bp.route('/gdrive-upload', methods=['POST'])
@authenticate
@validate_payload({
    "type": "object",
    "properties": {
        "file_url": {"type": "string", "format": "uri"},
        "filename": {"type": "string"},
        "folder_id": {"type": "string"},
        "mime_type": {"type": "string"},
        "chunk_size": {"type": "integer", "minimum": 1},
        "webhook_url": {"type": "string", "format": "uri"},
        "id": {"type": "string"}
    },
    "required": ["file_url", "filename", "folder_id"],
    "additionalProperties": False
})
@queue_task_wrapper(bypass_queue=False)
def gdrive_upload(job_id, data):
    logger.info(f"Processing Job ID: {job_id}")

    if not GDRIVE_USER:
        logger.error("GDRIVE_USER environment variable is not set")
        return "GDRIVE_USER environment variable is not set", "/gdrive-upload", 400

    try:
        file_url = data['file_url']
        filename = data['filename']
        folder_id = data['folder_id']
        mime_type = data.get('mime_type', 'application/octet-stream')
        chunk_size = data.get('chunk_size', 5 * 1024 * 1024)

        try:
            head_response = requests.head(file_url, allow_redirects=True, timeout=30)
            head_response.raise_for_status()
            total_size = int(head_response.headers.get('Content-Length', 0))
            get_response = requests.get(file_url, stream=True, timeout=30)
            get_response.raise_for_status()
            total_size = int(get_response.headers.get('Content-Length', 0))
            if total_size == 0:
                raise ValueError("Content-Length header is missing or zero")
        except requests.exceptions.RequestException as e:
            logger.error(f"Job {job_id}: Error accessing file URL: {str(e)}")
            return f"Error accessing file URL: {str(e)}", "/gdrive-upload", 500
        except ValueError as e:
            logger.error(f"Job {job_id}: {str(e)}")
            return f"Unable to determine file size: {str(e)}", "/gdrive-upload", 500

        logger.info(f"Job {job_id}: File size determined: {total_size} bytes")

        upload_url = initiate_resumable_upload(filename, folder_id, mime_type)
        logger.info(f"Job {job_id}: Resumable upload session initiated with chunk size {chunk_size} bytes.")

        file_id = upload_file_in_chunks(file_url, upload_url, total_size, job_id, chunk_size)

        return file_id, "/gdrive-upload", 200

    except Exception as e:
        logger.error(f"Job {job_id}: Error during processing - {str(e)}")
        return str(e), "/gdrive-upload", 500

def log_system_resources():
    while True:
        memory_info = psutil.virtual_memory()
        disk_info = psutil.disk_usage('/')

        with uploads_lock:
            for progress in active_uploads:
                with progress.lock:
                    percentage = (progress.bytes_uploaded / progress.total_size) * 100 if progress.total_size > 0 else 0
                    elapsed_time = time.time() - progress.start_time

                    if int(percentage) >= progress.last_logged_percentage + 1:
                        progress.last_logged_percentage = int(percentage)
                        logger.info(
                            f"Job {progress.job_id}: Uploaded {progress.bytes_uploaded} of {progress.total_size} bytes "
                            f"({percentage:.2f}%), Elapsed Time: {int(elapsed_time)} seconds"
                        )

                    if int(percentage) >= progress.last_logged_resource_percentage + 5:
                        progress.last_logged_resource_percentage = int(percentage)
                        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        logger.info(f"[{current_time}] Memory Usage: {memory_info.percent}% used")
                        logger.info(f"[{current_time}] Disk Usage: {disk_info.percent}% used")

        time.sleep(1)

resource_logging_thread = threading.Thread(
    target=log_system_resources,
    daemon=True
)
resource_logging_thread.start()
