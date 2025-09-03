"""
SCVMM Plugin related test cases.
"""

import glob
import json
import logging
import os

import pytest
from dotenv import load_dotenv
from hpe_glcp_automation_lib.libs.commons.utils.random_gens import RandomGenUtils
from hpe_morpheus_automation_libs.api.external_api.plugins.plugin_api import PluginAPI
from functional_tests.common.cloud_helper import ResourcePoller
from functional_tests.common.scvmm_utils import SCVMMUtils

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

load_dotenv()

host = os.getenv("BASE_URL")
admin_username = os.getenv("USERNAME")
admin_password = os.getenv("PASSWORD")

class TestSCVMMPlugin:
    """Test class for SCVMM plugin related functionalities."""

    group_id = None
    cloud_id = None
    instance_id = None

    def test_validate_windows_instance_creation_and_agent_installation_behaviour(
        self, morpheus_session
    ):
        """Test case to validate the creation of windows instance and agent installation behavior."""
        # This test case assumes that the SCVMM plugin is already registered and the necessary configurations are in place.

        instance_id = None  # Initialize instance_id to None for cleanup purposes
        try:

            plugin_api = PluginAPI(
                host=host, username=admin_username, password=admin_password
            )
            SCVMMUtils.upload_scvmm_plugin(plugin_api)

            # 2. create a scvmm group
            group_name = "test-scvmm-group-" + RandomGenUtils.random_string_of_chars(5)
            group_payload = {
                "group": {
                    "name": group_name,
                }
            }
            group_response = morpheus_session.groups.add_groups(group_payload)
            assert group_response.status_code == 200, "Group creation failed!"
            TestSCVMMPlugin.group_id = group_response.json()["group"]["id"]
            log.info(f"group_id: {TestSCVMMPlugin.group_id}")

            # 3. create a scvmm cloud under the created group
            TestSCVMMPlugin.cloud_id = SCVMMUtils.create_scvmm_cloud(
                morpheus_session,
                group_id=TestSCVMMPlugin.group_id,
            )
            # 4. register a scvmm cluster under the created cloud
            SCVMMUtils.create_scvmm_cluster(morpheus_session, TestSCVMMPlugin.cloud_id, TestSCVMMPlugin.group_id)

            # 5. create instance using windows2019 template
            log.info("creating instance")
            instance_id, instance_name = SCVMMUtils.create_instance(
                morpheus_session,
                group_id=TestSCVMMPlugin.group_id,
                cloud_id=TestSCVMMPlugin.cloud_id
            )
            assert instance_id is not None, "Instance creation failed!"
            log.info(f"Instance '{instance_name}' created successfully.")

            # 6. Verify the VM instance has been successfully deployed along with the agent
            details = SCVMMUtils.get_instance_details(morpheus_session, instance_id)
            agent_installed = details["instance"]["containerDetails"][0]["server"]["agentInstalled"]
            log.info(f"agent installed: {agent_installed}")
            assert agent_installed, "Agent installation failed on the instance."
            agent_version = details["instance"]["containerDetails"][0]["server"]["agentVersion"]
            log.info(f"Agent installed successfully on instance '{instance_id}' with version: {agent_version}")

            # 7. Start instance
            SCVMMUtils.perform_instance_operation(morpheus_session, instance_id, "start", "running")

            # 8. Restart instance
            SCVMMUtils.perform_instance_operation(morpheus_session, instance_id, "restart", "running")

            # 9. Stop instance
            SCVMMUtils.perform_instance_operation(morpheus_session, instance_id, "stop", "stopped")

            # 10. Start instance again
            SCVMMUtils.perform_instance_operation(morpheus_session, instance_id, "start", "running")
        except Exception as e:
            log.error(f"Test failed with exception: {e}")
            pytest.fail(f"Test failed with exception: {e}")

        finally:
            SCVMMUtils.cleanup_resource("instance", morpheus_session, instance_id)

    def test_validate_windows_instance_creation_with_selected_storage_and_host(
        self, morpheus_session
    ):
        """Test case to validate the creation of windows instance with selected storage and host."""
        # This test case assumes that the SCVMM plugin is already registered and the necessary configurations are in place.

        try:
            log.info("Creating instance with selected storage and host")
            instance_name = (
                "test-scvmm-instance-" + RandomGenUtils.random_string_of_chars(3)
            )
            group_id = TestSCVMMPlugin.group_id
            cloud_id = TestSCVMMPlugin.cloud_id
            log.info(f"Fetching host id for the instance creation")
            # Get the list of hosts
            hosts_response = morpheus_session.hosts.list_hosts(name="hyperv-node-44")
            assert hosts_response.status_code == 200, "Failed to retrieve hosts!"
            hosts_data = hosts_response.json()
            # Extract the host ID from the response
            if not hosts_data["servers"]:
                raise ValueError("No hosts found in the response.")
            host_id = hosts_data["servers"][0]["id"]
            log.info(f"Selected host ID: {host_id}")

            #  Step 2: Call the generic instance creation function
            instance_id, created_instance_name = SCVMMUtils.create_instance(
                morpheus_session=morpheus_session,
                instance_name=instance_name,
                group_id=group_id,
                cloud_id=cloud_id,
                host_id=host_id,
            )
            TestSCVMMPlugin.instance_id = instance_id
            log.info(f"Instance '{created_instance_name}' created successfully with ID {instance_id}.")

            # Step 3: Verify the instance details using helper
            details = SCVMMUtils.get_instance_details(
                morpheus_session,
                instance_id,
                assert_message="Failed to retrieve instance details after creation!"
            )
            # Verify the host ID in the instance details
            actual_host_id = details["instance"]["config"]["hostId"]
            log.info(f"Actual host ID in instance details: {actual_host_id}")
            assert (actual_host_id == host_id), f"Expected host ID {host_id}, but got {actual_host_id}."

        except Exception as e:
            log.error(f"Test failed with exception: {e}")
            pytest.fail(f"Test failed with exception: {e}")

    def test_validate_reconfigure_operation_on_deployed_windows_instance(
            self, morpheus_session
    ):
        """Test case to validate the reconfigure operation on a deployed windows instance."""
        instance_id = TestSCVMMPlugin.instance_id
        try:
            log.info(f"Reconfiguring instance with id '{instance_id}'...")
            # Step 1: Fetch instance details
            instance_details = SCVMMUtils.get_instance_details(
                morpheus_session, instance_id, "Failed to fetch instance details before reconfigure!"
            )
            # Step 2: Prepare payloads
            update_payload = SCVMMUtils.create_update_payload(labels=["Test1"])
            reconfigure_payload = SCVMMUtils.create_reconfigure_payload(
                instance_details=instance_details,
            )
            # Step 3: Update instance (labels)
            update_response = morpheus_session.instances.update_instance(
                id=instance_id, update_instance_request=update_payload
            )
            assert update_response.status_code == 200, f"Instance update operation failed: {update_response.text}"

            log.info("Waiting for instance to reach 'running' state after label update...")
            intermediate_status = ResourcePoller.poll_instance_status(
                instance_id, "running", morpheus_session
            )
            assert intermediate_status == "running", f"Instance didn't reach 'running'. Current: {intermediate_status}"

            # Step 4: Resize / Reconfigure instance
            resize_response = morpheus_session.instances.resize_instance(
                id=instance_id, resize_instance_request=reconfigure_payload
            )
            assert resize_response.status_code == 200, f"Instance resize operation failed: {resize_response.text}"

            log.info(f"Instance '{instance_id}' reconfigured successfully. Waiting for final state...")
            final_status = ResourcePoller.poll_instance_status(
                instance_id, "running", morpheus_session,
            )
            assert final_status == "running", f"Reconfigure failed with status: {final_status}"

            # Step 5: Validate after reconfigure
            final_details = SCVMMUtils.get_instance_details(
                morpheus_session, instance_id, "Failed to fetch instance details after reconfigure!"
            )
            # Expected values from payloads
            expected_plan_id = reconfigure_payload["instance"]["plan"]["id"]
            expected_volume_size = reconfigure_payload["volumes"][0]["size"]
            expected_labels = update_payload["instance"]["labels"]

            # Validations
            SCVMMUtils.validate_labels(final_details, expected_labels)
            SCVMMUtils.validate_volume_size(final_details, expected_volume_size)
            SCVMMUtils.validate_plan_id(final_details, expected_plan_id)
            log.info(
                f"Instance '{instance_id}' reconfigured successfully with "
                f"plan={expected_plan_id}, volume_size={expected_volume_size}, labels={expected_labels}"
            )
        except Exception as e:
            log.error(f"Test failed with exception: {e}")
            pytest.fail(f"Test failed with exception: {e}")

    def test_validate_clone_instance_operation_on_windows_instance_with_agent_install_not_skipped(
            self, morpheus_session
    ):
        """Test case to validate clone windows instance with agent install not skipped operation."""
        instance_id = TestSCVMMPlugin.instance_id
        cloned_instance_id= None
        try:
            clone_instance_name = f"clone-instance-{instance_id}"
            log.info(f"Cloning instance with id '{instance_id}'...")
            # Generate payload
            clone_payload = SCVMMUtils.create_clone_payload(
                clone_instance_name=clone_instance_name,
            )
            # Call clone API
            clone_response = morpheus_session.instances.clone_instance(
                id=instance_id, clone_instance_request=clone_payload
            )
            assert clone_response.status_code == 200, "Instance clone operation failed!"

            # Verify original instance is still running
            final_status = ResourcePoller.poll_instance_status(
                instance_id=instance_id,
                target_state="running",
                morpheus_session=morpheus_session,
            )
            assert final_status == "running", f"Original instance did not stay running. Current: {final_status}"

            # Fetch cloned instance ID
            clone_ins_response = morpheus_session.instances.list_instances(name=clone_instance_name)
            cloned_instance_id = clone_ins_response.json()["instances"][0]["id"]

            # Verify cloned instance is running
            final_status = ResourcePoller.poll_instance_status(
                instance_id=cloned_instance_id,
                target_state="running",
                morpheus_session=morpheus_session,
            )
            assert final_status == "running", f"Cloned instance creation failed with status: {final_status}"
            log.info(f"Cloned instance '{cloned_instance_id}' is running successfully.")

        except Exception as e:
            log.error(f"Test failed with exception: {e}")
            pytest.fail(f"Test failed with exception: {e}")
        finally:
            # Cleanup cloned instance
            if 'cloned_instance_id':
                SCVMMUtils.cleanup_resource("instance", morpheus_session, cloned_instance_id)

    def test_validate_backup_and_restore_operation_on_windows_instance(self, morpheus_session):
        """Test case to validate the backup and restore operation on a windows instance."""

        instance_id = TestSCVMMPlugin.instance_id
        backup_name = f"backup-instance-{instance_id}" + RandomGenUtils.random_string_of_chars(2)
        backup_job_name = f"backup-job-{instance_id}" + RandomGenUtils.random_string_of_chars(2)
        backup_id = None
        try:
            # Step 1: Get container ID from instance details
            instance_details = SCVMMUtils.get_instance_details(morpheus_session, instance_id)
            container_id = instance_details["instance"]["containers"][0]

            # Step 2: Create backup
            backup_payload = SCVMMUtils.create_backup_payload(instance_id, container_id, backup_name, backup_job_name)
            backup_response = morpheus_session.backups.add_backups(add_backups_request=backup_payload)
            assert backup_response.status_code == 200, "Instance backup operation failed!"
            backup_id = backup_response.json()["backup"]["id"]
            log.info(f"Backup job created with id {backup_id}, waiting for completion...")

            # Step 3: Execute backup job
            backup_job_response = morpheus_session.session.post(
                f"{morpheus_session.base_url}/api/backups/jobs/{backup_id}/execute",
                headers=morpheus_session.session.headers,
                json={}
            )
            assert backup_job_response.status_code == 200, "Failed to execute backup job!"

            # Step 4: Poll backup result
            final_backup_status = ResourcePoller.poll_backup_status(
                backup_id=backup_id,
                target_state="SUCCEEDED",
                morpheus_session=morpheus_session,
            )
            assert final_backup_status == "SUCCEEDED", f"Backup failed with status: {final_backup_status}"

            # Step 5: Get last backup result ID
            backup_results_response = morpheus_session.backups.get_backups(id=backup_id)
            assert backup_results_response.status_code == 200, "Failed to retrieve backup results!"
            last_backup_result_id = backup_results_response.json()["backup"]["lastResult"]["id"]

            # Step 6: Restore from backup
            restore_payload = SCVMMUtils.create_restore_payload(instance_id, last_backup_result_id)
            restore_response = morpheus_session.backups.execute_backup_restore(
                execute_backup_restore_request=restore_payload
            )
            assert restore_response.status_code == 200, "Instance restore operation failed!"
            restore_id = restore_response.json()["restore"]["id"]

            final_restore_status = ResourcePoller.poll_backup_restore_status(
                restore_id=restore_id,
                target_state="SUCCEEDED",
                morpheus_session=morpheus_session,
            )
            assert final_restore_status == "SUCCEEDED", f"Restore failed with status: {final_restore_status}"
            log.info(f"Instance '{instance_id}' restored successfully from backup.")

        except Exception as e:
            log.error(f"Test failed with exception: {e}")
            pytest.fail(f"Test failed with exception: {e}")

        finally:
            # Cleanup backup job
            if backup_id:
                SCVMMUtils.cleanup_resource("backup", morpheus_session, backup_id)
