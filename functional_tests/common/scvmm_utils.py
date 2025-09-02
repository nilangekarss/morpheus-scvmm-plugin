import json
import os
import glob
import pytest
import logging

from hpe_glcp_automation_lib.libs.commons.utils.random_gens import RandomGenUtils
from dotenv import load_dotenv
from functional_tests.common.cloud_helper import ResourcePoller

log = logging.getLogger(__name__)

load_dotenv()

def get_create_instance_payload(instance_name, template, group_id, cloud_id, host_id=None):
    """Helper function to create the payload for instance creation."""
    return {
        "instance": {
            "name": instance_name,
            "site": {"id": group_id},
            "instanceType": {"code": "scvmm"},
            "layout": {
                "id": int(os.getenv("SCVMM_LAYOUT_ID"))

            },
            "plan": {
                "id": int(os.getenv("PLAN_ID"))
            },
        },
        "zoneId": cloud_id,
        "networkInterfaces": [
            {
                "network": {
                    "id": str(os.getenv("NETWORK_ID"))
                }
            }
        ],
        "config": {
            "noAgent": False,
            "hostId": int(os.getenv("HOST_ID")),
            "template": int(template),
            "scvmmCapabilityProfile": "Hyper-V",
            "createUser": False,

        },
        "labels": ["TEST"],
        "volumes": json.loads(os.getenv("VOLUMES")) if os.getenv("VOLUMES") else [],
    }

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

def upload_scvmm_plugin(plugin_api, version="0.1.0"):
    """
    Uploads the SCVMM plugin JAR file using the provided PluginAPI instance.

    :param plugin_api: Instance of PluginAPI (already authenticated)
    :param version: Version of the plugin to upload (default: 0.1.0)
    :return: Response object from upload_plugin
    """
    current_dir = os.getcwd()
    jar_dir = os.path.join(current_dir, "build", "libs")
    log.info(f"Searching for plugin JAR in {jar_dir}")

    pattern = os.path.join(jar_dir, f"morpheus-scvmm-plugin-{version}-*.jar")
    matching_files = glob.glob(pattern)

    if len(matching_files) != 1:
        raise FileNotFoundError(
            f"Expected one JAR file, found {len(matching_files)}"
        )

    jar_file_path = matching_files[0]
    log.info(f"Found plugin JAR: {jar_file_path}")
    log.info("Uploading plugin...")

    try:
        plugin_response = plugin_api.upload_plugin(jar_file_path=jar_file_path)
        log.info(f"Response Status Code: {plugin_response.status_code}")
        assert (
            plugin_response.status_code == 200
        ), f"Plugin upload failed: {plugin_response.text}"
        log.info("Plugin uploaded successfully.")
        return plugin_response
    except Exception as e:
        log.error(f"Plugin upload failed: {e}")
        pytest.fail(f"Plugin upload failed: {e}")

def create_scvmm_cloud(morpheus_session, group_id):
    cloud_name = "test-scvmm-cloud-" + RandomGenUtils.random_string_of_chars(5)

    # 1. Get zone types
    zone_type_response = morpheus_session.clouds.list_cloud_types()
    assert zone_type_response.status_code == 200, "Failed to retrieve cloud types!"

    zone_types = zone_type_response.json().get("zoneTypes", [])
    zone_type_id = None
    for zone in zone_types:
        if zone.get("name") == "SCVMM":
            zone_type_id = zone.get("id")
            break

    assert zone_type_id is not None, "SCVMM zone type not found!"

    # 2. Build payload
    cloud_payload = get_create_cloud_payload(
        cloud_name=cloud_name, group_id=group_id, zone_type_id=zone_type_id
    )

    # 3. Create cloud
    cloud_response = morpheus_session.clouds.add_clouds(cloud_payload)
    assert cloud_response.status_code == 200, "Cloud creation failed!"
    cloud_id = cloud_response.json()["zone"]["id"]
    log.info(f"cloud_id: {cloud_id}")

    # 4. Poll until active
    final_status = ResourcePoller.poll_cloud_status(
        cloud_id=cloud_id,
        morpheus_session=morpheus_session,
    )
    assert final_status == "ok", f"Cloud creation failed with status: {final_status}"

    log.info(f"Cloud '{cloud_name}' created successfully with ID: {cloud_id}")
    return cloud_id

def create_scvmm_cluster(morpheus_session, cloud_id, group_id):
    cluster_name = "test-scvmm-cluster-" + RandomGenUtils.random_string_of_chars(5)

    # Fetching cluster-type ID for SCVMM
    cluster_type_response = morpheus_session.clusters.list_cluster_types()
    assert (
        cluster_type_response.status_code == 200
    ), "Failed to retrieve cluster types!"
    cluster_types = cluster_type_response.json().get("clusterTypes", [])
    cluster_type_id = None
    for cluster in cluster_types:
        if cluster.get("code") == "docker-cluster":
            cluster_type_id = cluster.get("id")
            break
    assert cluster_type_id is not None, "SCVMM cluster type not found!"
    log.info(f"Cluster type ID: {cluster_type_id}")

    # Fetching layout ID for SCVMM
    layout_response = morpheus_session.cluster_layouts.list_cluster_layouts()
    assert (
        layout_response.status_code == 200
    ), "Failed to retrieve cluster layouts!"
    layouts = layout_response.json().get("layouts", [])
    layout_id = None
    for layout in layouts:
        if layout.get("name") == "SCVMM Docker Host":
            layout_id = layout.get("id")
            log.info(f"Layout ID: {layout_id}")
            break
    assert layout_id is not None, "SCVMM cluster layout not found!"

    cluster_payload = {
        "cluster": {
            "name": cluster_name,
            "cloud": {"id": cloud_id},
            "type": {"id": cluster_type_id},
            "layout": {"id": layout_id},
            "server": {
                "id": int(os.getenv("SERVER_ID")),
                "name": cluster_name,
                "plan": {
                    "id": int(os.getenv("PLAN_ID"))
                },
                "config": {},
            },
            "group": {"id": group_id},
        }
    }

    cluster_response = morpheus_session.clusters.add_cluster(cluster_payload)

    if cluster_response.status_code == 200:
        cluster_id = cluster_response.json()["cluster"]["id"]
        log.info(f"cluster_id: {cluster_id}")

        final_status = ResourcePoller.poll_cluster_status(
            cluster_id=cluster_id,
            morpheus_session=morpheus_session,
        )

        if final_status == "ok":
            log.info(
                f"Cluster '{cluster_name}' registered successfully with ID: {cluster_id}"
            )
        else:
            log.warning(
                f"Cluster registration failed with status: {final_status}"
            )
    else:
        log.warning("Cluster registration failed! Could not create cluster.")

def create_instance(morpheus_session, instance_name=None, group_id=None, cloud_id=None, host_id=None):
    """
    Generic method to create an instance and wait until it's running.

    :param morpheus_session: Active Morpheus session
    :param instance_name: Optional name; if None, random name will be generated
    :param template: Template ID for instance (optional)
    :param group_id: Group ID for instance
    :param cloud_id: Cloud ID for instance
    :param host_id: Host ID for instance (optional)
    :return: (instance_id, instance_name)
    """
    log.info("Creating instance...")

    # Generate name if not provided
    if not instance_name:
        instance_name = "test-scvmm-instance-" + RandomGenUtils.random_string_of_chars(3)

    # Fetching template
    template_name = os.getenv("SCVMM_TEMPLATE_NAME")
    filter_type = "Synced"
    template_response = morpheus_session.library.list_virtual_images(name=template_name, filter_type=filter_type)
    assert template_response.status_code == 200, "Failed to retrieve templates!"
    template_data = template_response.json()
    log.info(f"template_data: {template_data}")
    template_id = template_data["virtualImages"][0]["id"]
    log.info(f"template_id: {template_id}")

    # Generate payload
    log.info("Generating instance payload...")
    create_instance_payload =get_create_instance_payload(
        instance_name=instance_name, template= template_id, group_id=group_id, cloud_id=cloud_id, host_id=host_id
    )
    log.info("Payload generated successfully")

    log.info(f"Instance payload: {json.dumps(create_instance_payload, indent=2)}")

    # Send request
    instance_response = morpheus_session.instances.add_instance(
        add_instance_request=create_instance_payload
    )

    if instance_response.status_code == 200:
        instance_id = instance_response.json()["instance"]["id"]
        log.info(f"Instance ID: {instance_id}")

        # Poll for instance status
        final_status = ResourcePoller.poll_instance_status(
            instance_id=instance_id,
            target_state="running",
            morpheus_session=morpheus_session,
        )

        if final_status == "running":
            instance_name = instance_response.json()["instance"]["name"]
            log.info(f"Instance '{instance_name}' created successfully with ID: {instance_id}")
            return instance_id, instance_name
        else:
            log.warning(f"Instance creation failed with status: {final_status}")
            return None, None
    else:
        log.warning("Instance creation failed! Could not create instance.")
        return None, None

def perform_instance_operation(
    morpheus_session,
    instance_id,
    operation: str,
    expected_status: str
):
    """
    Perform an operation (start, restart, stop) on a given instance
    and validate the final status.
    """
    log.info(f"Initiating '{operation}' operation on instance '{instance_id}'...")

    # Call correct API based on operation
    if operation == "start":
        response = morpheus_session.instances.start_instance(instance_id)
    elif operation == "restart":
        response = morpheus_session.instances.restart_instance(instance_id)
    elif operation == "stop":
        response = morpheus_session.instances.stop_instance(instance_id)
    else:
        raise ValueError(f"Unsupported operation: {operation}")

    # Validate initial API response
    assert response.status_code == 200, f"Instance {operation} operation failed!"

    # Poll until instance reaches expected state
    final_status = ResourcePoller.poll_instance_status(
        instance_id, expected_status, morpheus_session
    )

    assert (
        final_status == expected_status
    ), f"Instance failed to {operation}, current status: {final_status}"

    log.info(f"Instance '{instance_id}' {operation}ed successfully.")
    return final_status
