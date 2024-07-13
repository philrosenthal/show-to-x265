import argparse
import json
import logging
import os
import re
import subprocess
import threading
import time
from queue import Queue
from typing import List, Dict

logging.basicConfig(filename='encoding_log.txt', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def is_lossless_audio(codec_name: str) -> bool:
    """Determine if the audio codec is lossless compression."""
    lossless_codecs = ['flac', 'alac', 'dts-hd', 'truehd']
    return any(codec in codec_name.lower() for codec in lossless_codecs)

def parse_filename(filename: str) -> Dict[str, str]:
    """Parse the input filename to extract show name, season, episode, and episode name."""
    pattern = r"(.+)S(\d+)E(\d+)\s(.+?)\s(\d+p)"
    match = re.match(pattern, os.path.basename(filename))
    if match:
        return {
            "show_name": match.group(1).strip(),
            "season": match.group(2),
            "episode": match.group(3),
            "episode_name": match.group(4).strip(),
            "resolution": match.group(5)
        }
    return None

def generate_output_path(parsed_filename: Dict[str, str], output_dir: str) -> str:
    """Generate the output file path based on the parsed filename."""
    show_dir = os.path.join(output_dir, parsed_filename["show_name"])
    season_dir = os.path.join(show_dir, f"S{parsed_filename['season']}")
    output_file = f"E{parsed_filename['episode']}. {parsed_filename['episode_name']}.mkv"
    return os.path.join(season_dir, output_file)

def get_audio_channels(ffprobe_data: Dict[str, any]) -> List[int]:
    """Extract the number of channels for each audio stream."""
    return [stream["channels"] for stream in ffprobe_data["streams"] if stream["codec_type"] == "audio"]

def generate_ffmpeg_command(input_file: str, output_file: str, ffprobe_data: Dict[str, any]) -> List[str]:
    """Generate the ffmpeg command based on the input file properties."""
    cmd = [
        "nice", "-n", "20",
        "ffmpeg",
        "-i", input_file,
        "-map", "0",  # Map all streams from input to output
        "-c", "copy",  # Start by copying all streams
        "-c:v", "libx265",
        "-crf", "23",
        "-preset", "veryslow",
        "-x265-params", "rect=1:amp=1:bframes=16",
        "-f", "matroska"  # Explicitly specify the output format
    ]

    for stream in ffprobe_data["streams"]:
        if stream["codec_type"] == "audio":
            index = stream["index"]
            channels = stream.get("channels", 0)
            codec_name = stream["codec_name"]

            if channels >= 3 or not is_lossless_audio(codec_name):
                # Copy audio for 3+ channels or any non-lossless format
                cmd.extend([f"-c:a:{index}", "copy"])
            else:
                # Convert lossless 1.0 or 2.0 channel audio to FLAC
                cmd.extend([f"-c:a:{index}", "flac"])

    cmd.append(output_file)
    return cmd

def run_ffprobe(input_file: str) -> Dict[str, any]:
    """Run ffprobe on the input file and return the parsed output."""
    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        input_file
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return json.loads(result.stdout)

class EncodingJob:
    def __init__(self, input_file: str, output_file: str):
        self.input_file = input_file
        self.output_file = output_file
        self.start_time = None
        self.progress = 0
        self.estimated_time_remaining = None
        self.status = "Queued"

def worker(job_queue: Queue, active_jobs: Dict[str, EncodingJob]):
    """Worker function to process encoding jobs."""
    while True:
        job = job_queue.get()
        if job is None:
            break

        incomplete_file = f"{job.output_file}.incomplete"
        log_file = f"{os.path.splitext(job.output_file)[0]}.txt"

        try:
            job.status = "Analyzing"
            ffprobe_data = run_ffprobe(job.input_file)
            cmd = generate_ffmpeg_command(job.input_file, incomplete_file, ffprobe_data)

            job.start_time = time.time()
            job.status = "Encoding"
            with open(log_file, "w") as log:
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
                
                for line in process.stdout:
                    log.write(line)
                    log.flush()
                    
                    # Update progress
                    if "time=" in line:
                        time_match = re.search(r"time=(\d{2}):(\d{2}):(\d{2}\.\d{2})", line)
                        if time_match:
                            current_time = float(time_match.group(1)) * 3600 + float(time_match.group(2)) * 60 + float(time_match.group(3))
                            total_duration = float(ffprobe_data["format"]["duration"])
                            job.progress = min(current_time / total_duration * 100, 100)
                            
                            elapsed_time = time.time() - job.start_time
                            job.estimated_time_remaining = (elapsed_time / job.progress) * (100 - job.progress) if job.progress > 0 else None

            process.wait()
            
            if process.returncode == 0:
                os.rename(incomplete_file, job.output_file)
                job.status = "Completed"
                logging.info(f"Successfully encoded: {job.input_file} -> {job.output_file}")
            else:
                raise subprocess.CalledProcessError(process.returncode, cmd)

        except Exception as e:
            job.status = "Failed"
            logging.error(f"Error processing {job.input_file}: {str(e)}")
            if os.path.exists(incomplete_file):
                os.remove(incomplete_file)

        finally:
            job_queue.task_done()
            del active_jobs[job.input_file]

def display_status(active_jobs: Dict[str, EncodingJob]):
    """Display the current status of encoding jobs."""
    os.system('clear' if os.name == 'posix' else 'cls')
    print("Encoding Status:")
    for input_file, job in active_jobs.items():
        print(f"\n{os.path.basename(input_file)}:")
        print(f"  Status: {job.status}")
        if job.start_time:
            elapsed_time = time.time() - job.start_time
            print(f"  Progress: {job.progress:.2f}%")
            print(f"  Elapsed Time: {time.strftime('%H:%M:%S', time.gmtime(elapsed_time))}")
            if job.estimated_time_remaining:
                print(f"  Estimated Time Remaining: {time.strftime('%H:%M:%S', time.gmtime(job.estimated_time_remaining))}")

def main():
    parser = argparse.ArgumentParser(description="Re-encode video files using ffmpeg.")
    parser.add_argument("input_files", nargs="+", help="Input video files")
    parser.add_argument("output_dir", help="Output directory")
    args = parser.parse_args()

    job_queue = Queue()
    active_jobs = {}
    threads = []

    for _ in range(3):  # Create 3 worker threads
        t = threading.Thread(target=worker, args=(job_queue, active_jobs))
        t.start()
        threads.append(t)

    for input_file in args.input_files:
        parsed_filename = parse_filename(input_file)
        if parsed_filename:
            output_file = generate_output_path(parsed_filename, args.output_dir)
            if not os.path.exists(output_file):
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                job = EncodingJob(input_file, output_file)
                job_queue.put(job)
                active_jobs[input_file] = job
                logging.info(f"Queued for encoding: {input_file}")
            else:
                logging.info(f"Skipping {input_file}: Output file already exists")
        else:
            logging.warning(f"Skipping {input_file}: Unable to parse filename")

    # Add None to the queue to signal the workers to exit
    for _ in range(3):
        job_queue.put(None)

    # Display status
    while not job_queue.empty() or active_jobs:
        display_status(active_jobs)
        time.sleep(5)

    # Wait for all threads to complete
    for t in threads:
        t.join()

    print("All encoding jobs completed.")

if __name__ == "__main__":
    main()
