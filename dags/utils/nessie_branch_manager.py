import pynessie
import time
import os
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NessieBranchManager:
    def __init__(self, verify: bool = False):
        """Initialize the Nessie client for branch management."""
        self.endpoint = os.environ.get('NESSIE_ENDPOINT', "http://nessie:19120/api/v1/")
        self.nessie_client = NessieClient(config={"endpoint": self.endpoint, "verify": verify})

    def create_branch(self, branch_name: str, from_branch: str = "main"):
        """Create a new branch from the specified branch, ensuring the source branch hash is used."""
        try:
            # Check if the branch already exists
            existing_branch = self.nessie_client.get_reference(branch_name)
            logger.info(f"Branch '{branch_name}' already exists. Skipping creation.")
            return existing_branch.name  # Return the existing branch name
        except Exception:
            # If the branch doesn't exist, create it
            try:
                from_branch_ref = self.nessie_client.get_reference(from_branch)
                from_branch_hash = from_branch_ref.hash_
                new_branch = self.nessie_client.create_branch(branch_name, ref=from_branch, hash_on_ref=from_branch_hash)
                logger.info(f"Branch '{branch_name}' created from '{from_branch}' with hash '{from_branch_hash}'.")
                return new_branch.name  # Return the newly created branch name
            except Exception as e:
                logger.error(f"Failed to create branch '{branch_name}': {e}")
                raise

    def generate_custom_branch_name(self, table_name: str, label: str):
        """
        Generate a branch name with the format: bronze-customers-timestamp.
        """
        # Current timestamp
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        
        # Create the branch name
        branch_name = f"{label}-{table_name}-{timestamp}"
        return branch_name

    def delete_branch(self, branch_name: str):
        """Delete a branch."""
        try:
            branch_ref = self.nessie_client.get_reference(branch_name)
            branch_hash = branch_ref.hash_
            self.nessie_client.delete_branch(branch_name, branch_hash)
            logger.info(f"Branch '{branch_name}' deleted.")
        except Exception as e:
            logger.error(f"Failed to delete branch '{branch_name}': {e}")
            raise

    def merge_branch(self, from_branch: str, to_branch: str = "main"):
        """Merge a branch into another branch."""
        try:
            self.nessie_client.merge(from_ref=from_branch, onto_branch=to_branch)
            logger.info(f"Branch '{from_branch}' merged into '{to_branch}'.")
        except Exception as e:
            logger.error(f"Failed to merge branch '{from_branch}' into '{to_branch}': {e}")
            raise

    def get_branch(self, branch_name: str):
        """Get branch information."""
        try:
            branch = self.nessie_client.get_reference(branch_name)
            logger.info(f"Branch '{branch_name}' exists with hash: {branch.hash_}")
            return branch
        except Exception as e:
            logger.error(f"Failed to get branch '{branch_name}': {e}")
            raise