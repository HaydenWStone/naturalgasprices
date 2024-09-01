import subprocess
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Define repository details
local_repo_path = "/home/swieyeinthesky/naturalgasprices/"  # Replace with the path to your cloned repo
github_repo_url = os.getenv('github_repo_url')  # Replace with your GitHub repo URL and your token

# Define the branch you want to push to
branch = "main"

def run_command(command, cwd=None):
    result = subprocess.run(command, shell=True, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        print(f"Error running command: {command}")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        raise Exception(f"Command failed: {command}")
    return result.stdout

try:
    # Navigate to the local repository
    print("Navigating to the local repository...")
    run_command(f"cd {local_repo_path}")

    # Ensure we are on the correct branch
    print(f"Checking out branch {branch}...")
    run_command(f"git checkout -B {branch}", cwd=local_repo_path)

    # Add all changes
    print("Adding all changes...")
    run_command("git add -A", cwd=local_repo_path)

    # Commit the changes if there are any
    print("Committing the changes...")
    commit_output = run_command('git commit -m "Daily force push to update repo" || echo "nothing to commit"', cwd=local_repo_path)
    if "nothing to commit" in commit_output:
        print("Nothing to commit.")
    else:
        print(commit_output)

    # Force push to the remote GitHub repository
    print("Force pushing to the remote repository...")
    run_command(f"git push --force {github_repo_url} {branch}", cwd=local_repo_path)

    print("Force push completed successfully.")

except Exception as e:
    print(f"An error occurred: {e}")
