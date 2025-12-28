# hash:sha256:c861da6fcc751555e7516e5ba29bb36ac316147b42218fc39a27d42645125ac2
ARG REGISTRY_HOST
FROM $REGISTRY_HOST/codeocean/mambaforge3:24.5.0-0-python3.12.4-ubuntu22.04

ARG DEBIAN_FRONTEND=noninteractive
ARG GIT_ASKPASS
COPY git-askpass /

RUN --mount=type=secret,id=git-access-token \
    GIT_ACCESS_TOKEN="$(cat /run/secrets/git-access-token)" \
    pip3 install -U --no-cache-dir \
    aind-data-schema==2.3.0 \
    dandi==0.74.0 \
    hdmf==4.2.0 \
    nwbinspector==0.6.5 \
    pynwb==3.1.3 \
    quilt3==7.1.0 \
    -e git+https://github.com/hdmf-dev/hdmf-zarr@a8ce49065ba03d894e6cfd3f7e6b58d7b37fbd9b#egg=hdmf-zarr

ADD "https://github.com/coder/code-server/releases/download/v4.100.3/code-server-4.100.3-linux-amd64.tar.gz" /.code-server/code-server.tar.gz
	
RUN cd /.code-server \
    && tar -xvf code-server.tar.gz \
    && rm code-server.tar.gz \
    && ln -s /.code-server/code-server-4.100.3-linux-amd64/bin/code-server /usr/bin/code-server \
    && mkdir -p /.vscode/extensions
