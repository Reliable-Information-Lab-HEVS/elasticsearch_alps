# elasticsearch_alps

Welcome

The container_image folder contains the Dockerfile for the Container Image. Exact instructions on how to proceed are:
1. There is a created Dockerfile with desired packages, in “container_image/" folder 
2. Ensure you're in the directory containing the `Dockerfile` and run:
    podman build -t image:tag (⇒ you choose the desired image:tag)
3. enroot import -x mount -o <image_name.sqsh> podman://image:tag
4. Create .toml file in home directory /.edf path. 

    image = "/container_image/<image_name.sqsh>"
    workdir = "/capstor/scratch/cscs/<username>"
    writable = true
    mounts = [
        "/iopsstor/scratch/cscs/<username>:/iopsstor/scratch/cscs/<username>",
        "/capstor/scratch/cscs/<username>:/capstor/scratch/cscs/<username>",
        "/iopsstor/scratch/cscs/<username>/es-data:/usr/share/elasticsearch/data",
        "/iopsstor/scratch/cscs/<username>/es-logs:/usr/share/elasticsearch/logs"
    ]
    [annotations.com.hooks.ssh]
    enabled = "true"
    Explain that environment variables do not seem to be ingested at this point.
    Mount every directory you wish to be able to work from with this container.
    Absolutely need to connect to the elasticsearch data and logs folder
        
5. Open the container with: srun -A a-<account_number> --environment=<image_name> --pty bash
    1. Give instructions to verify versions

