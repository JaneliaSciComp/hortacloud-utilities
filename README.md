# hortacloud-utilities

This repository contains code for uploading neurons and associated JSON
to AWS S3.

## generate_upload_script.py

This program will generate shell scripts containing AWS CLI commands to copy
and sync files from the local file system to AWS S3. To run the program, you
must specify a sample date in the form YYYY-MM-DD:

```
python3 generate_upload_script.py --sample 2023-05-10
```

You will be prompted for the products to upload (defaults to images only):

```
[?] Enter products to upload:
 > [X] images
   [ ] registration
   [ ] segmentation
   [ ] tracings
   [ ] carveouts
```

One or two shell scripts will be generated:

* YYYY-MM-DD_images.sh : copies and syncs image files to AWS S3
* YYYY-MM-DD_cluster.sh : calls the compute cluster with *bsub* to copy/sync files to AWS S3

Products will be copied as follows:

### images

Images will be copied/synced from one of two base directories:

* ```/nrs/mouselight/SAMPLES/YYYY-MM-DD```
* ```/nearline/mouselight/data/RENDER_archive/YYYY-MM-DD```

In the base directory, the following files will be uploaded:

* sync files from ```ktx/``` to ```s3://janelia-mouselight-imagery/images/YYYY-MM-DD/ktx/```
* Copy the following files from base directory to ```s3://janelia-mouselight-imagery/images/YYYY-MM-DD```
  * ```default.0.tif```
  * ```default.1.tif```
  * ```tilebase.cache.yml```
  * ```transform.txt```

### registration

### segmentation

### tracings

### carveouts
