import os
import sys
import logging
import tempfile
import time
from datetime import datetime
import re

import requests
import whisper
from dotenv import load_dotenv

# ============== CONFIGURATION =====================
load_dotenv()

# The base URL of your FastAPI application
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")

# Set the Whisper model size ("tiny", "base", "small", "medium", "large")
WHISPER_MODEL = "base"

# Auto-detect or set the path to the ffmpeg executable
IS_CLOUD_SHELL = os.environ.get("CLOUD_SHELL", "") == "true"
IS_GITHUB_ACTIONS = os.environ.get("GITHUB_ACTIONS", "") == "true"

if os.name == 'nt': # Windows
    FFMPEG_PATH = r"C:\ProgramData\chocolatey\bin"
else:
    FFMPEG_PATH = "/usr/bin"
    if IS_CLOUD_SHELL or IS_GITHUB_ACTIONS:
        FFMPEG_PATH = "/usr/bin"  # Default location in Linux/Cloud Shell/GitHub Actions
        # If running in GitHub Actions, we'll need to install ffmpeg - but with sudo
        if IS_GITHUB_ACTIONS:
            try:
                def log(msg, level="info"):
                    print(f"[{level.upper()}] {msg}")
                log("Checking if ffmpeg is already installed...", "info")
                import subprocess
                ffmpeg_check = subprocess.run(['ffmpeg', '-version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                log("ffmpeg is already installed.", "info")
            except Exception:
                log("ffmpeg not found, will be installed by the workflow instead", "info")
                # Note: We don't try to install it here as we need sudo privileges
                # It will be installed by the GitHub Action workflow
os.environ["PATH"] += os.pathsep + FFMPEG_PATH


# ============== LOGGING SETUP =====================
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"transcript_worker_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============== API COMMUNICATION ==================
def get_videos_to_transcribe():
    """Fetches a list of videos needing transcription from the API."""
    url = f"{API_BASE_URL}/api/videos_to_transcribe"
    try:
        logger.info(f"Requesting videos from: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to connect to the API to fetch videos: {e}")
        return None

def update_video_transcript(video_id: int, transcript: str):
    """Submits the completed transcript back to the API."""
    url = f"{API_BASE_URL}/api/update_transcript/{video_id}"
    payload = {"transcript": transcript}
    print("payload", payload)
    try:
        response = requests.put(url, json=payload, timeout=30)
        response.raise_for_status()
        logger.info(f"Successfully updated transcript for video ID: {video_id}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to update transcript for video ID {video_id}: {e}")
        return False


# ============ VIDEO PROCESSING UTILITIES ====================
def download_video(url: str, out_dir: str) -> str | None:
    """Downloads a video from a URL with retries."""
    # (This function is the same as the previous version)
    try:
        filename = url.split("/")[-1].split("?")[0]
        sanitized_filename = re.sub(r'[^a-zA-Z0-9._-]', '_', filename)
        local_filename = os.path.join(out_dir, sanitized_filename)
        logger.info(f"Downloading video: {url}")
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        if os.path.getsize(local_filename) > 0:
            logger.info(f"Successfully downloaded to {local_filename}")
            return local_filename
        return None
    except Exception as e:
        logger.error(f"Failed to download video {url}: {e}")
        return None


def transcribe_video(file_path: str, model) -> str:
    """Transcribes a video file using Whisper."""
    try:
        logger.info(f"Transcribing {os.path.basename(file_path)}...")
        result = model.transcribe(file_path, fp16=False)
        transcript_text = result["text"].strip()
        logger.info(f"Transcription successful for {os.path.basename(file_path)}")
        return transcript_text if transcript_text else "[Empty Transcript]"
    except Exception as e:
        logger.error(f"Failed to transcribe video {file_path}: {e}")
        return f"[Transcription Error: {e}]"


# ============ MAIN WORKER LOGIC ======================
def main():
    """Main worker function."""
    logger.info("==== TRANSCRIPT WORKER START ====")

    # 1. Fetch videos from the API
    videos = get_videos_to_transcribe()
    if videos is None:
        logger.error("Could not fetch data from API. Shutting down.")
        return

    if not videos:
        logger.info("No new videos to transcribe. All done!")
        return

    total_videos = len(videos)
    logger.info(f"Found {total_videos} videos to process.")

    # 2. Load the Whisper model once
    logger.info(f"Loading Whisper model '{WHISPER_MODEL}'...")
    model = whisper.load_model(WHISPER_MODEL)
    logger.info("Whisper model loaded successfully.")

    processed_count = 0
    failed_count = 0

    # 3. Process each video
    for i, video_data in enumerate(videos):
        video_id = video_data.get("id")
        media_url = video_data.get("media_url")

        if not all([video_id, media_url]):
            logger.warning(f"Skipping invalid video record: {video_data}")
            continue

        logger.info(f"--- Processing video {i+1}/{total_videos} (ID: {video_id}) ---")

        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = download_video(media_url, tmpdir)
            if video_path:
                transcript = transcribe_video(video_path, model)
                if update_video_transcript(video_id, transcript):
                    processed_count += 1
                else:
                    failed_count += 1
            else:
                # Mark as failed if download fails
                update_video_transcript(video_id, "[Processing Error: Download Failed]")
                failed_count += 1
    
    logger.info("==== TRANSCRIPT WORKER END ====")
    logger.info(f"Summary: Processed={processed_count}, Failed={failed_count}, Total={total_videos}")


if __name__ == "__main__":
    # Check for ffmpeg before starting
    try:
        import subprocess
        subprocess.run(['ffmpeg', '-version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    except (FileNotFoundError, subprocess.SubprocessError):
        logger.critical("FATAL ERROR: ffmpeg is not installed or not in your PATH.")
        sys.exit(1)

    main()
    