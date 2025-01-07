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

If a sample is not specified, you will be prompted for one.

You will be prompted for the products to upload (defaults to images only):

```
[?] Enter products to upload:
 > [X] images
   [ ] registration
   [ ] segmentation
   [ ] tracings
   [ ] carveouts
```

One to three shell scripts will be generated:

* ```YYYY-MM-DD_images.sh``` : copies and syncs image files to AWS S3
* ```YYYY-MM-DD_cluster.sh``` : calls the compute cluster with *bsub* to sync registration, segmentation, and tracings files to AWS S3
* ```YYYY-MM-DD_carveouts.sh```: calls the compute cluster with *bsub* to sync carveout files to AWS S3

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

Uses *bsub* to sync files from ```/groups/mousebrainmicro/mousebrainmicro/registration/Database/YYYY-MM-DD/``` to ```s3://janelia-mouselight-imagery/registration/YYYY-MM-DD/```

### segmentation

Uses *bsub* to sync files from ```/groups/mousebrainmicro/mousebrainmicro/cluster/Reconstructions/YYYY-MM-DD/stitching-output/``` to ```s3://janelia-mouselight-imagery/segmentation/YYYY-MM-DD/```

### tracings

Uses *bsub* to sync files from ```/groups/mousebrainmicro/mousebrainmicro/tracing_complete/YYYY-MM-DD/``` to ```s3://janelia-mouselight-imagery/tracings/tracing_complete/YYYY-MM-DD/```.

Uses *bsub* to sync files from ```/groups/mousebrainmicro/mousebrainmicro/shared_tracing/Finished_Neurons/YYYY-MM-DD/``` to ```s3://janelia-mouselight-imagery/tracings/Finished_Neurons/YYYY-MM-DD/```.

### carveouts

Uses *bsub* to sync files from ```/nrs/funke/mouselight/YYYY-MM-DD/``` to ```s3://janelia-mouselight-imagery/carveouts/YYYY-MM-DD/```.

Uses *bsub* to sync files from ```/nrs/funke/mouselight-v2/YYYY-MM-DD/``` to ```s3://janelia-mouselight-imagery/carveouts/YYYY-MM-DD/```.

## update_aws_neurons.py

This program will create and upload neuron data JSON files to the
create and upload neuron data JSON files. The program first calls NeuronBrowser
to create a mapping of dates to neurons.

The user is them prompted to process finished neurons and/or tracing complete
neurons.

### Finished neurons
Metadata files are created for each date and uploaded to ```s3://janelia-mouselight-imagery/neurons/Finished_Neurons/YYYY-MM-DD/metadata.json```. The metadata file will contain a list of neurons (with their original names) for that date. Example:

```
{"title": "2014-06-24 MouseLight published neurons",
 "neurons": {"AA0230": {"originalName": "G-001"},
             "AA0231": {"originalName": "G-002"},
             "AA0232": {"originalName": "G-003"},
             "AA0233": {"originalName": "G-004"},
             "AA0234": {"originalName": "G-005"},
             "AA0235": {"originalName": "G-006"},
             "AA0236": {"originalName": "G-007"},
             "AA0237": {"originalName": "G-009"},
             "AA0238": {"originalName": "G-010"},
             "AA0239": {"originalName": "G-011"},
             "AA0243": {"originalName": "G-012"},
             "AA0240": {"originalName": "G-013"},
             "AA0241": {"originalName": "G-014"},
             "AA0242": {"originalName": "G-015"}
            }
}
```
