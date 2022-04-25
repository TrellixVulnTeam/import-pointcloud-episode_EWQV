import os
import sys

import supervisely as sly
from dotenv import load_dotenv
from supervisely.app.v1.app_service import AppService

app_root_directory = os.path.dirname(os.getcwd())
sys.path.append(app_root_directory)
sys.path.append(os.path.join(app_root_directory, "src"))
print(f"App root directory: {app_root_directory}")
sly.logger.info(f'PYTHONPATH={os.environ.get("PYTHONPATH", "")}')
# order matters
load_dotenv(os.path.join(app_root_directory, "secret_debug.env"))
load_dotenv(os.path.join(app_root_directory, "debug.env"))


api: sly.Api = sly.Api.from_env()
my_app: AppService = AppService()

TASK_ID = int(os.environ["TASK_ID"])
TEAM_ID = os.environ["context.teamId"]
WORKSPACE_ID = os.environ["context.workspaceId"]
INPUT_DIR = os.environ.get("modal.state.slyFolder")
INPUT_FILE = os.environ.get("modal.state.slyFile")
assert INPUT_DIR or INPUT_FILE

storage_dir: str = my_app.data_dir
sly.fs.mkdir(storage_dir, True)
