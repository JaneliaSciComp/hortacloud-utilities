''' This program will create shell scripts to upload sample files to AWS S3. One file is
    created for use on a general-purpose machine, and one for use on the cluster. The
    AWS CLI is required for the sheel scripts to run.
'''

import argparse
import glob
import re
import os
import sys
import colorlog
import inquirer

BASE = "/groups/mousebrainmicro/mousebrainmicro"
IMAGE_BASE = ["/nrs/mouselight/SAMPLES", "/nearline/mouselight/data/RENDER_archive"]
CARVEOUT_BASE = ["/nrs/funke/mouselight", "/nrs/funke/mouselight-v2"]
BUCKET = "s3://janelia-mouselight-imagery"


def get_target(base_dir, suffix=None):
    ''' Generate a target prefix for AWS S3
        Keyword arguments:
          base_dir: base prefix
          suffix: suffix
        Returns:
          AWS S3 target prefix
    '''
    trg = re.search("\d{4}-\d{2}-\d{2}", ARG.SAMPLE)[0]
    dlist = [BUCKET, base_dir, trg]
    if suffix:
        dlist.append(suffix)
    return "/".join(dlist)


def process_images(base, img):
    ''' Write copy commands for images.
        Keyword arguments:
          base: images base directory
          img: image file handle
        Returns:
          None
    '''
    LOGGER.info(f"Using images from {base}")
    source = "/".join([base, "ktx/"])
    target = get_target("images", "ktx/")
    if os.path.exists(source):
        img.write(f"aws s3 sync {source} {target} --only-show-errors --profile FlyLightPDSAdmin\n")
    else:
        LOGGER.warning("Could not find %s", source)
    target = get_target("images")
    for file in ["default.0.tif", "default.1.tif", "tilebase.cache.yml", "transform.txt"]:
        source = "/".join([base, file])
        if os.path.exists(source):
            img.write(f"aws s3 cp {source} {target}/ --only-show-errors "
                      + "--profile FlyLightPDSAdmin\n")
        else:
            LOGGER.warning("Could not find %s", source)


def process_registration(clu):
    ''' Write copy commands for registration files.
        Keyword arguments:
          clu: cluster file handle
        Returns:
          None
    '''
    mmdd = "".join(ARG.SAMPLE.split("-")[1:3])
    source = "/".join([BASE, "registration/Database", ARG.SAMPLE])
    clu.write("echo 'Uploading registration'\n")
    if os.path.exists(source):
        target = get_target("registration")
        clu.write(f"bsub -J reg{mmdd} -n 4 -P mouselight 'aws s3 sync "
                  + f"{source}/ {target}/ --only-show-errors --profile FlyLightPDSAdmin'\n")
    else:
        LOGGER.warning("Could not find %s", source)


def process_segmentation(clu):
    ''' Write copy commands for segmentation files.
        Keyword arguments:
          clu: cluster file handle
        Returns:
          None
    '''
    mmdd = "".join(ARG.SAMPLE.split("-")[1:3])
    done = False
    prefix = "/".join([BASE, 'cluster/Reconstructions', ARG.SAMPLE])
    suffix = []
    while not done:
        question = [inquirer.Text("suffix", message="segmantation suffix(es)")
                   ]
        answer = inquirer.prompt(question)
        if answer["suffix"]:
            source = "/".join([prefix, answer["suffix"]])
            if os.path.exists(source):
                suffix.append(source)
            else:
                LOGGER.warning("Could not find %s", source)
        else:
            done = True
    #target = "/".join([BUCKET, f"segmentation/{ARG.SAMPLE}"])
    target = get_target("segmentation")
    counter = 1
    clu.write("echo 'Uploading segmentation'\n")
    for source in suffix:
        clu.write(f"bsub -J seg{mmdd}-{str(counter)} -n 4 -P mouselight 'aws s3 sync "
                  + f"{source}/ {target}/ --only-show-errors --profile FlyLightPDSAdmin'\n")
        counter += 1


def process_tracings(clu):
    ''' Write copy commands for tracing files.
        Keyword arguments:
          clu: cluster file handle
        Returns:
          None
    '''
    mmdd = "".join(ARG.SAMPLE.split("-")[1:3])
    counter = 1
    clu.write("echo 'Uploading tracings'\n")
    for sub in ["Finished_Neurons", "tracing_complete"]:
        sub2 = sub
        if sub == "Finished_Neurons":
            sub2 = "shared_tracing/Finished_Neurons"
        source = "/".join([BASE, sub2, ARG.SAMPLE])
        if os.path.exists(source):
            #target = "/".join([BUCKET, f"tracings/{sub}/{ARG.SAMPLE}"])
            target = get_target(f"tracings/{sub}")
            clu.write(f"bsub -J tra{mmdd}-{str(counter)} -n 4 -P mouselight 'aws s3 sync "
                      + f"{source}/ {target}/ --only-show-errors --profile FlyLightPDSAdmin'\n")
            counter += 1
        else:
            LOGGER.warning("Could not find %s", source)


def check_ktx(base):
    ''' Check that the ktx directory is valid and uses current naming conventions.
        Keyword arguments:
          base: images base directory
        Returns:
          True if the ktx directory is valid, False if not
    '''
    base += "/ktx"
    files = os.listdir(base)
    return "block_8_xy_.ktx" in files


def get_sample():
    ''' Allow the user to select a sample.
        Keyword arguments:
          None
        Returns:
          None
    '''
    if not ARG.SAMPLE:
        sample_date = []
        for test_base in IMAGE_BASE:
            for smp in glob.glob(test_base + "/*/ktx"):
                sdate = smp.split("/")[-2]
                if re.search(r"^\d\d\d\d-\d\d-\d\d", sdate):
                    sample_date.append(sdate)
        sample_date.sort(reverse=True)
        sample_date.insert(0, "(Enter manually)")
        question = [inquirer.List("sample",
                                  message="Sample date",
                                  choices=sample_date,
                                  default=sample_date[1]
                                 )]
        answer = inquirer.prompt(question)
        ARG.SAMPLE = answer["sample"]


def process_carveouts(crv):
    ''' Write copy commands for carveouts.
        Keyword arguments:
          crv: carveouts file handle
        Returns:
          None
    '''
    for base in CARVEOUT_BASE:
        cbase = "/".join([base, ARG.SAMPLE])
        if not os.path.exists(cbase):
            LOGGER.warning("Could not find %s", cbase)
            continue
        LOGGER.info(f"Using carveouts from {cbase}")
        source = cbase + "/"
        target = get_target("carveouts")
        if os.path.exists(cbase):
            print(source, target)
            crv.write(f"aws s3 sync {source} {target} --only-show-errors --profile FlyLightPDSAdmin\n")
    

def process_sample():
    ''' Process the specified sample to create upload files.
        Keyword arguments:
          None
        Returns:
          None
    '''
    get_sample()
    question = [inquirer.Checkbox("products",
                                  message="Enter products to upload",
                                  choices=["images", "registration", "segmentation", "tracings", "carveouts"],
                                  default=["images"],
                                 )
               ]
    answer = inquirer.prompt(question)
    products = answer["products"]
    if "images" in products:
        found = False
        for test_base in IMAGE_BASE:
            ibase = "/".join([test_base, ARG.SAMPLE])
            if os.path.exists(ibase):
                found = True
                break
        if not found:
            question = [inquirer.Text("base", message="images base directory")
                       ]
            answer = inquirer.prompt(question)
            ibase = answer["base"]
            if not os.path.exists("/".join([ibase, "ktx"])):
                LOGGER.error("Could not find ktx directory in %s", ibase)
                sys.exit(-1)
        if not check_ktx(ibase):
            LOGGER.error("Image files under %s use an obsolete naming scheme", ibase)
            sys.exit(-1)
        with open(f"{ARG.SAMPLE}_images.sh", "w", encoding="utf8") as img:
            process_images(ibase, img)
    if any(itm in products for itm in ["registration", "segmentation", "tracings"]):
        with open(f"{ARG.SAMPLE}_cluster.sh", "w", encoding="utf8") as clu:
            if "registration" in products:
                process_registration(clu)
            if "segmentation" in products:
                process_segmentation(clu)
            if "tracings" in products:
                process_tracings(clu)
    if "carveouts" in products:
        with open(f"{ARG.SAMPLE}_carveouts.sh", "w", encoding="utf8") as crv:
            process_carveouts(crv)


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Generate command files to upload MouseLight data")
    PARSER.add_argument('--sample', dest='SAMPLE', action='store',
                        help='Sample date')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()

    LOGGER = colorlog.getLogger()
    ATTR = colorlog.colorlog.logging if "colorlog" in dir(colorlog) else colorlog
    if ARG.DEBUG:
        LOGGER.setLevel(ATTR.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(ATTR.INFO)
    else:
        LOGGER.setLevel(ATTR.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)

    process_sample()
    sys.exit(0)
