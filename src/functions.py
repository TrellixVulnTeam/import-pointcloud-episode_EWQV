import os
import shutil
import globals as g
from pathlib import Path

import download_progress
import supervisely as sly


def download_input_files(api, task_id, input_dir, input_file):
    if input_dir:
        sizeb = api.file.get_directory_size(g.TEAM_ID, input_dir)
        cur_files_path = input_dir
        extract_dir = os.path.join(g.storage_dir, cur_files_path.lstrip("/").rstrip("/"))
        input_dir = extract_dir
        project_name = Path(cur_files_path).name

        progress_cb = download_progress.get_progress_cb(api, task_id, f"Downloading {g.INPUT_DIR.lstrip('/').rstrip('/')}", sizeb,
                                                        is_size=True)
        api.file.download_directory(g.TEAM_ID, cur_files_path, extract_dir, progress_cb)
    else:
        sizeb = api.file.get_info_by_path(g.TEAM_ID, input_file).sizeb
        cur_files_path = input_file
        archive_path = os.path.join(g.storage_dir, sly.fs.get_file_name_with_ext(cur_files_path))
        extract_dir = os.path.join(g.storage_dir, sly.fs.get_file_name(cur_files_path))
        input_dir = extract_dir
        project_name = sly.fs.get_file_name(input_file)

        progress_cb = download_progress.get_progress_cb(api, task_id, f"Downloading {g.INPUT_FILE.lstrip('/')}",
                                                        sizeb,
                                                        is_size=True)
        api.file.download(g.TEAM_ID, cur_files_path, archive_path, None, progress_cb)

        shutil.unpack_archive(archive_path, extract_dir)
        sly.fs.silent_remove(archive_path)

        if len(os.listdir(g.storage_dir)) > 1:
            g.my_app.logger.error("There must be only 1 project directory in the archive")
            raise Exception("There must be only 1 project directory in the archive")
    return input_dir, project_name
