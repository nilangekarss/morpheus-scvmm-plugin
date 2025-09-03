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
from functional_tests.common.scvmm_utils import upload_scvmm_plugin, create_scvmm_cloud, create_scvmm_cluster, \
    create_instance, perform_instance_operation


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
            upload_scvmm_plugin(plugin_api)

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
            TestSCVMMPlugin.cloud_id = create_scvmm_cloud(
                morpheus_session,
                group_id=TestSCVMMPlugin.group_id,
            )
            # 4. register a scvmm cluster under the created cloud
            create_scvmm_cluster(morpheus_session, TestSCVMMPlugin.cloud_id, TestSCVMMPlugin.group_id)

            # 5. create instance using windows2019 template
            log.info("creating instance")
            instance_id, instance_name = create_instance(
                morpheus_session,
                group_id=TestSCVMMPlugin.group_id,
                cloud_id=TestSCVMMPlugin.cloud_id
            )
            assert instance_id is not None, "Instance creation failed!"
            log.info(f"Instance '{instance_name}' created successfully.")

            # 6. Verify the VM instance has been successfully deployed along with the agent
            agent_response = morpheus_session.instances.get_instance(id=instance_id)
            log.info(f"Response Status Code: {agent_response.status_code}")
            assert (
                agent_response.status_code == 200
            ), "Failed to retrieve instance details!"
            details = agent_response.json()
            agent_installed = details["instance"]["containerDetails"][0]["server"][
                "agentInstalled"
            ]
            log.info(f"agent installed: {agent_installed}")
            assert agent_installed, "Agent installation failed on the instance."
            agent_version = agent_response.json()["instance"]["containerDetails"][0][
                "server"
            ]["agentVersion"]
            log.info(
                f"Agent installed successfully on instance '{instance_id}' with version: {agent_version}"
            )

            # 7. Start instance
            perform_instance_operation(morpheus_session, instance_id, "start", "running")

            # 8. Restart instance
            perform_instance_operation(morpheus_session, instance_id, "restart", "running")

            # 9. Stop instance
            perform_instance_operation(morpheus_session, instance_id, "stop", "stopped")

            # 10. Start instance again
            perform_instance_operation(morpheus_session, instance_id, "start", "running")
        except Exception as e:
            log.error(f"Test failed with exception: {e}")
            pytest.fail(f"Test failed with exception: {e}")

        finally:
            # Cleanup: Delete the instance if it was created
            if instance_id:
                log.info(f"Cleaning up instance '{instance_id}'...")
                try:
                    delete_response = morpheus_session.instances.delete_instance(
                        id=instance_id
                    )
                    if delete_response.status_code == 200:
                        log.info(f"Instance '{instance_id}' deleted successfully.")
                    else:
                        log.warning(
                            f"Failed to delete instance '{instance_id}': {delete_response.text}"
                        )
                except Exception as e:
                    log.error(f"Cleanup failed with exception: {e}")
            else:
                log.info("No instance was created, so nothing to clean up.")

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
            instance_id, created_instance_name = create_instance(
                morpheus_session=morpheus_session,
                instance_name=instance_name,
                group_id=group_id,
                cloud_id=cloud_id,
                host_id=host_id  # Pass specific host ID
            )

            TestSCVMMPlugin.instance_id = instance_id
            log.info(f"Instance '{created_instance_name}' created successfully with ID {instance_id}.")

            # Step 3: Verify the instance details to confirm the selected host is used
            get_instance_response = morpheus_session.instances.get_instance(
                id=instance_id
            )
            assert (
                get_instance_response.status_code == 200
            ), "Failed to retrieve instance details!"
            details = get_instance_response.json()
            # Verify the host ID in the instance details
            actual_host_id = details["instance"]["config"]["hostId"]
            log.info(f"Actual host ID in instance details: {actual_host_id}")
            assert (
                actual_host_id == host_id
            ), f"Expected host ID {host_id}, but got {actual_host_id}."

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
            #fetching volume_id
            instance_res= morpheus_session.instances.get_instance(id=instance_id)
            instance_res_json= instance_res.json()
            volume_id= instance_res_json["instance"]["volumes"][0]["id"]
            update_payload = {
                "instance": {
                    "labels": ["Test1"]
                }
            }
            reconfigure_payload = {
                "instance": {
                    "plan": {
                        "id": 164
                    }
                },
                "volumes": [
                    {
                        "size": 85,
                        "id": volume_id,
                        "name": "root",
                        "rootVolume": True,
                        "storageType": 1,
                    }
                ],
                "networkInterfaces": [
                    {
                        "network": {"id": "2"}
                    }
                ]
            }
            update_response = morpheus_session.instances.update_instance(
                id=instance_id, update_instance_request=update_payload
            )
            assert (
                update_response.status_code == 200
            ), f"Instance update operation failed: {update_response.text}"
            #  Wait for instance to be stable (e.g., 'running') before resizing
            log.info("Waiting for instance to reach 'running' state after label update...")
            intermediate_status = ResourcePoller.poll_instance_status(
                instance_id, "running", morpheus_session
            )
            assert intermediate_status == "running", (
                f"Instance didn't reach 'running' after label update. Current status: {intermediate_status}"
            )
            # Proceed with resize operation
            resize_response = morpheus_session.instances.resize_instance(
                id=instance_id, resize_instance_request=reconfigure_payload
            )
            assert (
                    resize_response.status_code == 200
            ), f"Instance resize operation failed: {resize_response.text}"
            log.info(
                f"Instance '{instance_id}' reconfigured successfully. Waiting for status..."
            )
            final_status = ResourcePoller.poll_instance_status(
                instance_id, "running", morpheus_session,
            )
            assert (
                    final_status == "running"
            ), f"Instance reconfigure failed with status: {final_status}"

            # Capture expected values dynamically from payloads
            expected_plan_id = reconfigure_payload["instance"]["plan"]["id"]
            expected_volume_size = reconfigure_payload["volumes"][0]["size"]
            expected_labels = update_payload["instance"]["labels"]
            # Fetch final instance details
            final_instance_details = morpheus_session.instances.get_instance(id=instance_id)
            assert final_instance_details.status_code == 200, "Failed to retrieve instance details after reconfigure!"
            final_details = final_instance_details.json()

            # Validate label update (if label update was part of your update_payload)
            final_labels = final_details["instance"].get("labels", [])
            for label in expected_labels:
                assert label in final_labels, f"Expected label '{label}' not found in {final_labels}"

            # Validate volume size
            final_volume_size = final_details["instance"]["volumes"][0]["size"]
            assert (
                    final_volume_size == expected_volume_size
            ), f"Expected volume size {expected_volume_size}, but got {final_volume_size}"

            # Validate plan
            final_plan_id = final_details["instance"]["plan"]["id"]
            assert (
                    final_plan_id == expected_plan_id
            ), f"Expected plan ID {expected_plan_id}, but got {final_plan_id}"

            log.info(
                f"Instance '{instance_id}' reconfigured successfully with plan={final_plan_id}, "
                f"volume_size={final_volume_size}, labels={final_labels}"
            )
            log.info(
                f"Instance '{instance_id}' reconfigured successfully with new volume size and labels."
            )
        except Exception as e:
            log.error(f"Test failed with exception: {e}")
            pytest.fail(f"Test failed with exception: {e}")


    def test_validate_clone_instance_operation_on_windows_instance_with_agent_install_not_skipped(self,
                                                                                                  morpheus_session):
        """ Test case to validate clone windows instance with agent install not skipped operation."""
        instance_id = TestSCVMMPlugin.instance_id
        try:

            clone_instance_name= f"clone-instance-{instance_id}"

            log.info(f"Cloning instance with id '{instance_id}'...")
            clone_payload = {

                "name": clone_instance_name,
                "plan": {
                    "id": 163
                },
                "volumes": [
                    {
                        "datastoreId": "auto",
                        "name": "root",
                        "size": 80,
                        "rootVolume": True

                    }]

            }
            clone_response = morpheus_session.instances.clone_instance(
                id=instance_id, clone_instance_request=clone_payload
            )
            assert clone_response.status_code == 200, "Instance clone operation failed!"
            final_status = ResourcePoller.poll_instance_status(
                instance_id=instance_id,
                target_state="running",
                morpheus_session=morpheus_session,
            )
            assert (
                    final_status == "running"
            ), f"Cloned instance creation started: {final_status}"

            clone_ins_response= morpheus_session.instances.list_instances(name=clone_instance_name)
            cloned_instance_id = clone_ins_response.json()["instances"][0]["id"]

            # Polling for cloned instance status
            final_status = ResourcePoller.poll_instance_status(
                instance_id=cloned_instance_id,
                target_state="running",
                morpheus_session=morpheus_session,
            )
            assert (
                    final_status == "running"
            ), f"Cloned instance creation failed with status: {final_status}"
            log.info(f"Cloned instance '{cloned_instance_id}' is running successfully.")

        except Exception as e:
            log.error(f"Test failed with exception: {e}")
            pytest.fail(f"Test failed with exception: {e}")

    def test_validate_backup_and_restore_operation_on_windows_instance(self, morpheus_session):
        """Test case to validate the backup and restore operation on a windows instance."""

        instance_id = TestSCVMMPlugin.instance_id
        backup_name= f"backup-instance-{instance_id}" + RandomGenUtils.random_string_of_chars(2)
        backup_job_name= f"backup-job-{instance_id}"+ RandomGenUtils.random_string_of_chars(2)
        backup_id = None

        try:
            log.info(f"Creating backup for instance with id {instance_id}")
            #fetching containerId
            instance_res= morpheus_session.instances.get_instance(id=instance_id)
            instance_res_json= instance_res.json()
            container_id= instance_res_json["instance"]["containers"][0]
            backup_payload = { "backup": {
                            "locationType": "instance",
                            "backupType": "scvmmSnapshot",
                            "jobAction": "new",
                            "name": backup_name,
                            "instanceId": instance_id,
                            "retentionCount": 2,
                            "jobName": backup_job_name,
                            "containerId": container_id
                 } }
            backup_response = morpheus_session.backups.add_backups(add_backups_request=backup_payload)
            assert backup_response.status_code == 200, "Instance backup operation failed!"
            backup_id = backup_response.json()["backup"]["id"]
            log.info(f"Backup job created with id {backup_id}, waiting for completion...")

            # Execute backup job
            backup_job_response = morpheus_session.session.post(
                f"{morpheus_session.base_url}/api/backups/jobs/{backup_id}/execute",
                headers=morpheus_session.session.headers,
                json={}
            )
            assert backup_job_response.status_code == 200, "Failed to execute backup job!"

            #polling backup result
            final_backup_status = ResourcePoller.poll_backup_status(
                backup_id=backup_id,
                target_state="SUCCEEDED",
                morpheus_session=morpheus_session,
            )
            assert (
                    final_backup_status == "SUCCEEDED"
            ), f"Instance backup failed with status: {final_backup_status}"

            #fetching last backup result id to trigger restore
            backup_results_response = morpheus_session.backups.get_backups(id=backup_id)
            assert backup_results_response.status_code == 200, "Failed to retrieve backup results!"
            backup_result= backup_results_response.json()
            last_backup_result_id= backup_result["backup"]["lastResult"]["id"]

            #executing backup restore
            log.info(f"Restoring backup for instance with id {instance_id}")
            restore_payload= {"restore": {
                    "restoreInstance": "existing",
                    "backupResultId": last_backup_result_id,
                    "instanceId": instance_id
                } }
            restore_response= morpheus_session.backups.execute_backup_restore(execute_backup_restore_request=restore_payload)
            assert restore_response.status_code == 200, "Instance restore operation failed!"
            log.info(f"Restore job initiated for instance with id {instance_id}, waiting for completion...")

            #fetchng backup_restore_id
            restore_id= restore_response.json()["restore"]["id"]
            #polling restore result
            final_restore_status = ResourcePoller.poll_backup_restore_status(
                restore_id=restore_id,
                target_state="SUCCEEDED",
                morpheus_session=morpheus_session,
            )
            assert (
                    final_restore_status == "SUCCEEDED"
            ), f"Instance restore failed with status: {final_restore_status}"
            log.info(f"Instance '{instance_id}' restored successfully from backup.")
        except Exception as e:
            log.error(f"Test failed with exception: {e}")
            pytest.fail(f"Test failed with exception: {e}")
        finally:
            # delete backup if created
            if backup_id:
                log.info(f"Cleaning up backup '{backup_id}'...")
                try:
                    delete_response = morpheus_session.backups.remove_backups(
                        id=backup_id
                    )
                    if delete_response.status_code == 200:
                        log.info(f"Backup '{backup_id}' deleted successfully.")
                    else:
                        log.warning(
                            f"Failed to delete backup '{backup_id}': {delete_response.text}"
                        )
                except Exception as e:
                    log.error(f"Cleanup failed with exception: {e}")
            else:
                log.info("No backup was created, so nothing to clean up.")
