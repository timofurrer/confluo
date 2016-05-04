publish:
	@python3.5 setup.py sdist bdist_wheel upload

docker_rabbit:
	sudo docker run -d --hostname confluo-rabbit --name confluo-rabbit rabbitmq:3-management
