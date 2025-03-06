# version_controller.py （云端适配版）
import subprocess

class GitOpsController:
    def __init__(self, repo_url="https://github.com/user/repo"):
        self.repo_path = "/workspace/repo"  # 使用云端固定路径
        if not os.path.exists(self.repo_path):
            subprocess.run(["git", "clone", repo_url, self.repo_path])

    async def check(self):
        subprocess.run(["git", "-C", self.repo_path, "fetch"])
        # 其余代码与文档相同
