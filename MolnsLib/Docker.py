import logging
import re
import time

from MolnsLib.Utils import Log
from molns_provider import ProviderBase
from Constants import Constants
from docker import Client
from docker.errors import NotFound, NullResource, APIError


class Docker:

    """ A wrapper over docker-py."""

    LOG_TAG = "Docker"

    shell_commands = ["source"]

    class ImageBuildException(Exception):
        def __init__(self, message=None):
            super("Something went wrong while building docker container image.\n{0}".format(message))

    def __init__(self):
        self.client = Client(base_url=Constants.DOCKER_BASE_URL)
        self.build_count = 0
        logging.basicConfig(level=logging.DEBUG)

    def create_container(self, image_id=Constants.DOCKER_DEFAULT_IMAGE, port_bindings={80: 8080}):
        """Creates a new container with elevated privileges. Returns the container ID. Maps port 80 of container
        to 8080 of locahost by default"""

        Log.write_log("Using image {0}".format(image_id))
        hc = self.client.create_host_config(privileged=True, port_bindings=port_bindings)
        container = self.client.create_container(image=image_id, command="/bin/bash", tty=True, detach=True, ports=[80],
                                                 host_config=hc)
        return container.get("Id")

    def stop_containers(self, container_ids):
        """Stops given containers."""
        for container_id in container_ids:
            self.stop_container(container_id)

    def stop_container(self, container_id):
        """Stops the container with given ID."""
        self.client.stop(container_id)

    def container_status(self, container_id):
        """Checks if container with given ID running."""
        status = ProviderBase.STATUS_TERMINATED
        try:
            ret_val = str(self.client.inspect_container(container_id).get('State').get('Status'))
            if ret_val.startswith("running"):
                status = ProviderBase.STATUS_RUNNING
            else:
                status = ProviderBase.STATUS_STOPPED
        except NotFound:
            pass
        return status

    def start_containers(self, container_ids):
        """Starts each container in given list of container IDs."""
        for container_id in container_ids:
            self.start_container(container_id)

    def start_container(self, container_id):
        """ Start the container with given ID."""
        Log.write_log(Docker.LOG_TAG + " Starting container " + container_id)
        try:
            self.client.start(container=container_id)
        except (NotFound, NullResource) as e:
            Log.write_log(Docker.LOG_TAG + " Something went wrong while starting container.", e)
            return False
        return True

    def execute_command(self, container_id, command):
        """Executes given command as a shell command in the given container. Returns None is anything goes wrong."""
        run_command = "/bin/bash -c \"" + command + "\""
        # print("CONTAINER: {0} COMMAND: {1}".format(container_id, run_command))
        if self.start_container(container_id) is False:
            Log.write_log(Docker.LOG_TAG + "Could not start container.")
            return None
        try:
            exec_instance = self.client.exec_create(container_id, run_command)
            response = self.client.exec_start(exec_instance)
            return [self.client.exec_inspect(exec_instance), response]
        except (NotFound, APIError) as e:
            Log.write_log(Docker.LOG_TAG + " Could not execute command.", e)
            return None

    def build_image(self, dockerfile):
        """ Build image from given Dockerfile object and return ID of the image created. """
        Log.write_log(Docker.LOG_TAG + "Building image...")
        image_tag = Constants.DOCKER_IMAGE_PREFIX + "{0}".format(self.build_count)
        last_line = ""
        try:
            for line in self.client.build(fileobj=dockerfile, rm=True, tag=image_tag):
                print(line)
                if "errorDetail" in line:
                    raise Docker.ImageBuildException()
                last_line = line

            # Return image ID. It's a hack around the fact that docker-py's build image command doesn't return an image
            # id.
            exp = r'[a-z0-9]{12}'
            image_id = re.findall(exp, str(last_line))[0]
            Log.write_log(Docker.LOG_TAG + "Image ID: {0}".format(image_id))
            return [image_id, image_tag]
        except (Docker.ImageBuildException, IndexError) as e:
            print("ERROR {0}".format(e))
            return None

    def image_exists(self, image_id):
        """Checks if an image with the given ID exists locally."""
        for image in self.client.images():
            some_id = image["Id"]
            some_tags = image["RepoTags"]
            if image_id in some_id[:(Constants.DOCKER_PY_IMAGE_ID_PREFIX_LENGTH + Constants.DOKCER_IMAGE_ID_LENGTH)]:
                return True
            if image_id in some_tags:
                return True
        return False

    def terminate_containers(self, container_ids):
        """ Terminates containers with given container ids."""
        for container_id in container_ids:
            try:
                if self.container_status(container_id) == ProviderBase.STATUS_RUNNING:
                    self.stop_container(container_id)
                self.terminate_container(container_id)
            except NotFound:
                pass

    def terminate_container(self, container_id):
        self.client.remove_container(container_id)

    def put_archive(self, container_id, tar_file_bytes, target_path_in_container):
        """ Copies and unpacks a given tarfile in the container at specified location.
        Location must exist in container."""
        if self.start_container(container_id) is False:
            Log.write_log(Docker.LOG_TAG + "ERROR Could not start container.")
            return

        # Prepend file path with /home/ubuntu/. TODO Should be refined.
        if not target_path_in_container.startswith("/home/ubuntu/"):
            target_path_in_container = "/home/ubuntu/" + target_path_in_container

        if not self.client.put_archive(container_id, target_path_in_container, tar_file_bytes):
            Log.write_log(Docker.LOG_TAG + "Failed to copy.")

    def get_container_ip_address(self, container_id):
        """ Returns the IP Address of given container."""
        self.start_container(container_id)
        ins = self.client.inspect_container(container_id)
        ip_address = str(ins.get("NetworkSettings").get("IPAddress"))
        while True:
            ip_address = str(ins.get("NetworkSettings").get("IPAddress"))
            if ip_address == "":
                time.sleep(3)
            if ip_address.startswith("1") is True:
                break
        return ip_address
