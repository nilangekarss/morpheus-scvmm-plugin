import os
import logging

log = logging.getLogger(__name__)


class CommonUtils:
    """ Common utility functions for functional tests. """

    @staticmethod
    def get_scvmm_instance_layout_id(morpheus_session):
        """Fetch the first SCVMM layout ID using the SCVMM layout code from env."""
        response = morpheus_session.library.list_layouts(code=os.getenv("SCVMM_LAYOUT_CODE"))
        assert response.status_code == 200, "Failed to retrieve cluster layouts!"

        layouts = response.json().get("instanceTypeLayouts", [])
        if not layouts:
            raise ValueError("No layouts found for the given SCVMM layout code!")
        layout_id = layouts[0].get("id")
        log.info(f"Layout ID: {layout_id}")
        return layout_id

    @staticmethod
    def get_network_id(morpheus_session):
        """Fetch the network ID for a given network name."""
        response = morpheus_session.networks.list_networks(name=os.getenv("NETWORK_NAME"))
        assert response.status_code == 200, "Failed to retrieve networks!"

        networks = response.json().get("networks", [])
        if networks:
            return networks[0].get("id")

        raise ValueError(f"Network not found!")

    @staticmethod
    def get_backup_job_details(morpheus_session, backup_id):
        """Fetch backup job details by backup ID."""
        response = morpheus_session.session.get(
            f"{morpheus_session.base_url}/api/backups/{backup_id}",
            headers=morpheus_session.session.headers
        )
        response.raise_for_status()
        return response.json()
