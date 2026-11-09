# Step 9 local mode: the "machine" every container runs (Ubuntu 24.04 like the EC2 image, with what the Ubuntu AMI already has)
FROM ubuntu:24.04
RUN apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3 curl openssl procps ca-certificates iproute2 >/dev/null && rm -rf /var/lib/apt/lists/*
