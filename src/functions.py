import os
import shutil
from pathlib import Path

import supervisely as sly
from supervisely.api.module_api import ApiField
from supervisely.imaging.image import SUPPORTED_IMG_EXTS
from supervisely.io.fs import list_files
from supervisely.io.json import load_json_file
from supervisely.pointcloud_annotation.pointcloud_episode_annotation import (
    PointcloudEpisodeAnnotation,
)
from supervisely.project.project_type import ProjectType
from supervisely.task.progress import Progress
from supervisely.video_annotation.key_id_map import KeyIdMap

import download_progress
import globals as g


def download_input_files(api: sly.Api, task_id: int, input_dir: str, input_file: str):
    """Downloads file or directory by given path from Team Files."""
    if input_dir:
        sizeb = api.file.get_directory_size(g.TEAM_ID, input_dir)
        cur_files_path = input_dir
        extract_dir = os.path.join(
            g.storage_dir, cur_files_path.lstrip("/").rstrip("/")
        )
        input_dir = extract_dir
        project_name = Path(cur_files_path).name

        progress_cb = download_progress.get_progress_cb(
            api,
            task_id,
            f"Downloading {g.INPUT_DIR.lstrip('/').rstrip('/')}",
            sizeb,
            is_size=True,
        )
        api.file.download_directory(g.TEAM_ID, cur_files_path, extract_dir, progress_cb)
    else:
        sizeb = api.file.get_info_by_path(g.TEAM_ID, input_file).sizeb
        cur_files_path = input_file
        archive_path = os.path.join(
            g.storage_dir, sly.fs.get_file_name_with_ext(cur_files_path)
        )
        extract_dir = os.path.join(g.storage_dir, sly.fs.get_file_name(cur_files_path))
        input_dir = extract_dir
        project_name = sly.fs.get_file_name(input_file)

        progress_cb = download_progress.get_progress_cb(
            api, task_id, f"Downloading {g.INPUT_FILE.lstrip('/')}", sizeb, is_size=True
        )
        api.file.download(g.TEAM_ID, cur_files_path, archive_path, None, progress_cb)

        shutil.unpack_archive(archive_path, extract_dir)
        sly.fs.silent_remove(archive_path)

        if len(os.listdir(g.storage_dir)) > 1:
            g.my_app.logger.error(
                "There must be only 1 project directory in the archive"
            )
            raise Exception("There must be only 1 project directory in the archive")
    return input_dir, project_name


def upload_episodes(
    api: sly.Api,
    dataset_id: int,
    dataset_name: str,
    item_paths: list,
    item_names: list,
    frame_pcd_map: dict,
    log_progress: bool,
):
    """Upload pointcloud episodes files."""
    items_infos = {"names": [], "paths": [], "metas": []}

    for item_path, item_name in zip(item_paths, item_names):
        pcd_to_frame = {v: k for k, v in frame_pcd_map.items()}
        item_to_ann = {name: pcd_to_frame[name] for name in item_names}
        frame_idx = int(item_to_ann[item_name])

        item_meta = {"frame": frame_idx}

        items_infos["names"].append(item_name)
        items_infos["paths"].append(item_path)
        items_infos["metas"].append(item_meta)

    ds_progress = (
        Progress(
            "Uploading pointclouds: {!r}".format(dataset_name),
            total_cnt=len(item_names),
        )
        if log_progress
        else None
    )
    pcd_infos = api.pointcloud_episode.upload_paths(
        dataset_id,
        names=items_infos["names"],
        paths=items_infos["paths"],
        metas=items_infos["metas"],
        progress_cb=ds_progress.iters_done_report if log_progress else None,
    )

    return pcd_infos


def upload_annotations(
    api: sly.Api,
    dataset_id: int,
    pcd_infos: list,
    episode_ann: PointcloudEpisodeAnnotation,
    uploaded_objects: KeyIdMap,
):
    """Upload pointclouds annotations."""
    frame_to_pcd_ids = {pcd_info.frame: pcd_info.id for pcd_info in pcd_infos}
    api.pointcloud_episode.annotation.append(
        dataset_id, episode_ann, frame_to_pcd_ids, uploaded_objects
    )


def upload_photo_context(
    api: sly.Api,
    dataset_name: str,
    pcd_infos: list,
    base_related_images_path: str,
    log_progress: bool,
):
    """Upload photo context for pointclouds."""
    img_infos = {"img_paths": [], "img_metas": []}

    for pcd_info in pcd_infos:
        related_images_dir = os.path.join(
            base_related_images_path, pcd_info.name.replace(".", "_")
        )
        related_items_paths = sorted(list_files(related_images_dir, SUPPORTED_IMG_EXTS))

        img_infos["img_paths"].extend(related_items_paths)

    img_progress = (
        Progress(
            "Uploading photo context: {!r}".format(dataset_name),
            total_cnt=len(img_infos["img_paths"]),
        )
        if log_progress
        else None
    )

    images_hashes = api.pointcloud_episode.upload_related_images(
        img_infos["img_paths"],
        progress_cb=img_progress.iters_done_report if log_progress else None,
    )

    images_hashes_iterator = images_hashes.__iter__()
    for pcd_info in pcd_infos:
        related_images_dir = os.path.join(
            base_related_images_path, pcd_info.name.replace(".", "_")
        )
        related_items_meta_paths = sorted(list_files(related_images_dir, [".json"]))

        for meta_json_path in related_items_meta_paths:
            img_hash = next(images_hashes_iterator)
            meta_json = load_json_file(meta_json_path)
            img_infos["img_metas"].append(
                {
                    ApiField.ENTITY_ID: pcd_info.id,
                    ApiField.NAME: meta_json[ApiField.NAME],
                    ApiField.HASH: img_hash,
                    ApiField.META: meta_json[ApiField.META],
                }
            )

    api.pointcloud_episode.add_related_images(img_infos["img_metas"])


def upload_pointcloud_episode_project(
    api, project_dir, workspace_id, project_name, log_progress=False
):
    project_remotely = api.project.create(
        workspace_id,
        project_name,
        ProjectType.POINT_CLOUD_EPISODES,
        change_name_if_conflict=True,
    )

    project_meta_path = os.path.join(project_dir, "meta.json")
    project_meta_json = load_json_file(project_meta_path)
    project_meta = sly.ProjectMeta.from_json(project_meta_json)

    api.project.update_meta(project_remotely.id, project_meta_json)

    uploaded_objects = KeyIdMap()
    datasets_names = [
        dataset_name
        for dataset_name in os.listdir(project_dir)
        if os.path.isdir(os.path.join(project_dir, dataset_name))
    ]
    for dataset_name in datasets_names:
        dataset_path = os.path.join(project_dir, dataset_name)
        item_dir = os.path.join(dataset_path, "pointcloud")
        rimage_dir = os.path.join(dataset_path, "related_images")

        item_paths = sorted(
            [
                os.path.join(item_dir, item_name)
                for item_name in os.listdir(item_dir)
                if item_name.endswith(".pcd")
            ]
        )
        item_names = sorted(
            [
                item_name
                for item_name in os.listdir(item_dir)
                if item_name.endswith(".pcd")
            ]
        )

        frame_pcd_map_path = os.path.join(dataset_path, "frame_pointcloud_map.json")
        if os.path.isfile(frame_pcd_map_path):
            frame_pcd_map = load_json_file(frame_pcd_map_path)
        else:
            sly.logger.info(
                f"File: {frame_pcd_map_path} doesn't exists, frame pointcloud map will be generated automatically"
            )
            frame_pcd_map = {
                frame_index: item_names[frame_index]
                for frame_index in range(len(item_names))
            }

        ann_json_path = os.path.join(dataset_path, "annotation.json")
        if os.path.isfile(ann_json_path):
            ann_json = load_json_file(ann_json_path)
            episode_annotation = PointcloudEpisodeAnnotation.from_json(
                ann_json, project_meta
            )
        else:
            episode_annotation = PointcloudEpisodeAnnotation()

        dataset_remotely = api.dataset.create(
            project_remotely.id,
            dataset_name,
            description=episode_annotation.description,
            change_name_if_conflict=True,
        )

        pcd_infos = upload_episodes(
            api=api,
            dataset_id=dataset_remotely.id,
            dataset_name=dataset_remotely.name,
            item_paths=item_paths,
            item_names=item_names,
            frame_pcd_map=frame_pcd_map,
            log_progress=log_progress,
        )

        upload_annotations(
            api=api,
            dataset_id=dataset_remotely.id,
            pcd_infos=pcd_infos,
            episode_ann=episode_annotation,
            uploaded_objects=uploaded_objects,
        )

        upload_photo_context(
            api=api,
            dataset_name=dataset_remotely.name,
            pcd_infos=pcd_infos,
            base_related_images_path=rimage_dir,
            log_progress=log_progress,
        )

    return project_remotely.id, project_remotely.name
