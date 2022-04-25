import supervisely as sly

import functions as f
import globals as g


@g.my_app.callback("import_pointcloud_episode")
@sly.timeit
def import_pointcloud_episode(api: sly.Api, task_id, context, state, app_logger):
    input_dir, project_name = f.download_input_files(
        api=api, task_id=task_id, input_dir=g.INPUT_DIR, input_file=g.INPUT_FILE
    )

    project_id, project_name = f.upload_pointcloud_episode_project(
        api=api,
        project_dir=input_dir,
        workspace_id=g.WORKSPACE_ID,
        project_name=project_name,
        log_progress=True,
    )
    api.task.set_output_project(
        task_id=task_id, project_id=project_id, project_name=project_name
    )
    g.my_app.stop()


def main():
    sly.logger.info(
        "Script arguments",
        extra={
            "context.teamId": g.TEAM_ID,
            "context.workspaceId": g.WORKSPACE_ID,
            "modal.state.slyFolder": g.INPUT_DIR,
            "modal.state.slyFile": g.INPUT_FILE,
        },
    )

    g.my_app.run(initial_events=[{"command": "import_pointcloud_episode"}])


if __name__ == "__main__":
    sly.main_wrapper("main", main)
