"""pytest plugin: a stand-in for the `docker` package, so a suite that starts `mongo:latest` in a container
(LlamaIndex mongo chat store) talks to whatever already listens on localhost:27017 (mongod or our server).
Loaded with `-p fake_docker`; nothing else in the suite changes."""
import sys
import types


class _Container:
    def stop(self):
        pass

    def remove(self):
        pass


class _Containers:
    def run(self, *args, **kwargs):
        return _Container()


class _Client:
    containers = _Containers()

    def close(self):
        pass


docker = types.ModuleType("docker")
docker.from_env = lambda: _Client()
models = types.ModuleType("docker.models")
containers = types.ModuleType("docker.models.containers")
containers.Container = _Container
docker.models = models
models.containers = containers
sys.modules.update({"docker": docker, "docker.models": models, "docker.models.containers": containers})
