# dev_stack

Local development stack support files.

This directory is **not** a Rust crate and is **not** part of the Cargo
workspace. It exists to hold support assets for local bring-up, such as:

- development `compose.yaml`
- local certificate material under `ca/`
- Docker-related ignore rules

This directory used to be named `main_lib/`, which was misleading after the
workspace crate split. The new name is meant to make its role obvious.
