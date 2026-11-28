---
section: Added
---

- **The Docker image includes boto3 for AWS reads** (QV-589, part 5). Every image now installs the `aws` extra,
  whatever `TRAILBEAM_EXTRAS` it is built with, so turning AWS reads on in Settings needs no rebuild. The client
  library already brings botocore, so boto3 adds 3.4 MB to a 312 MB image. The reads stay off by default, and boto3
  is not imported until they are on. With pip, add the extra: `pip install "trailbeam[server,aws]"`. The README has
  a new section, "AWS reads": turning them on, the credentials and the three read-only permissions they need.
