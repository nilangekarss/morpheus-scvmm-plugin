import json
import os

from functional_tests.common.common_utils import CommonUtils


class SCVMMpayloads:
    """ Helper class to create payloads for SCVMM related operations """

    @staticmethod
    def get_create_instance_payload(morpheus_session, instance_name, template, group_id, cloud_id, host_id=None):
        """Helper function to create the payload for instance creation."""

        instance_layout_id = CommonUtils.get_scvmm_instance_layout_id(morpheus_session)
        network_id = CommonUtils.get_network_id(morpheus_session)
        return {
            "instance": {
                "name": instance_name,
                "site": {"id": group_id},
                "instanceType": {"code": "scvmm"},
                "layout": {
                    "id": instance_layout_id

                },
                "plan": {
                    "id": int(os.getenv("PLAN_ID"))
                },
            },
            "zoneId": cloud_id,
            "networkInterfaces": [
                {
                    "network": {
                        "id": str(network_id)
                    }
                }
            ],
            "config": {
                "noAgent": False,
                "hostId": host_id,
                "template": int(template),
                "scvmmCapabilityProfile": "Hyper-V",
                "createUser": False,
                "backup": {"providerBackupType": int(os.getenv("BACKUP_TYPE_ID"))
                           }
            },
            "labels": ["TEST"],
            "volumes": json.loads(os.getenv("VOLUMES")) if os.getenv("VOLUMES") else [],
        }

    @staticmethod
    def get_create_cloud_payload(cloud_name, group_id, zone_type_id):
        """Helper function to create the payload for cloud creation."""
        return {
            "zone": {
                "name": cloud_name,
                "credential": {"type": "local"},
                "zoneType": {"id": zone_type_id},
                "groups": {"id": group_id},
                "groupId": group_id,
                "config": {
                    "host": os.getenv("HOST"),
                    "username": os.getenv("HOST_USERNAME"),
                    "password": os.getenv("HOST_PASSWORD"),
                    "sharedController": os.getenv("SHARED_CONTROLLER"),
                },
            }
        }

    @staticmethod
    def create_update_payload(labels):
        """Create payload for updating instance labels."""
        return {
            "instance": {
                "labels": labels
            }
        }

    @staticmethod
    def create_reconfigure_payload(instance_details):
        """Create payload for reconfiguring an instance."""
        volume = instance_details["instance"]["volumes"][0]

        return {
            "instance": {
                "plan": {
                    "id": 163
                }
            },
            "volumes": [
                {
                    "size": 85,
                    "id": volume["id"],
                    "name": volume["name"],
                    "rootVolume": volume.get("rootVolume", True),
                    "storageType": 1,
                }
            ],
            "networkInterfaces": [
                {
                    "network": {"id": os.getenv("NETWORK_ID")},
                }
            ]
        }

    @staticmethod
    def create_clone_payload(clone_instance_name: str):
        """
        Create payload for cloning an instance.

        :param clone_instance_name: Name for the cloned instance
        :return: Clone payload dictionary
        """
        return {
            "name": clone_instance_name,
            "volumes": [
                {
                    "datastoreId": "auto",
                    "size": 90,
                }
            ]
        }

    @staticmethod
    def create_backup_payload(instance_id, container_id, backup_name, backup_job_name, schedule_id):
        """
        Create payload for backup creation.
        """
        return {
            "backup": {
                "locationType": "instance",
                "backupType": "scvmmSnapshot",
                "jobAction": "new",
                "jobSchedule": schedule_id,
                "name": backup_name,
                "instanceId": instance_id,
                "retentionCount": 2,
                "jobName": backup_job_name,
                "containerId": container_id
            }
        }

    @staticmethod
    def create_restore_payload(instance_id, last_backup_result_id):
        """Generate payload for restoring a backup."""
        return {
            "restore": {
                "restoreInstance": "existing",
                "backupResultId": last_backup_result_id,
                "instanceId": instance_id,
            }
        }
