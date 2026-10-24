# Wraps the damon-relay binary you copied next to this file — no
# build step, no base-image toolchain. Works on glibc hosts
# (debian bookworm); switch to alpine if you built a musl binary.
FROM debian:bookworm-slim
COPY damon-relay /usr/local/bin/damon-relay
RUN chmod +x /usr/local/bin/damon-relay
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/damon-relay"]
