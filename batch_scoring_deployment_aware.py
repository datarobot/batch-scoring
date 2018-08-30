import sys

from datarobot_batch_scoring import main
# This exists as a simple entrypoint for PyInstaller

sys.exit(main.main_deployment_aware())
