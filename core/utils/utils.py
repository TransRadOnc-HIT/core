import os
import pydicom
import glob
import re
import pydicom as pd
import requests
import tarfile


def check_dcm_dose(dcms):

    right_dcm = []
    for dcm in dcms:
        hd = pydicom.read_file(dcm)
        try:
            hd.GridFrameOffsetVector
            hd.pixel_array
            right_dcm.append(dcm)
        except:
            continue
    return right_dcm

def string_strip(string, expression):
    return string.strip(expression)

def check_rtstruct(basedir, regex):

    data = []
    no_rt = []
    no_match = []
    for root, _, files in os.walk(basedir):
        for name in files:
            if (('RTDOSE' in name and name.endswith('.nii.gz'))
                    and os.path.isdir(os.path.join(root, 'RTSTRUCT_used'))):
                sub_name = root.split('/')[-2]
                try:
                    rts = glob.glob(os.path.join(root, 'RTSTRUCT_used', '*.dcm'))[0]
                    matching = check_rts(rts, regex)
                    if matching:
                        data.append(sub_name)
                    else:
                        no_match.append(sub_name)
                except IndexError:
                    print('No RTSTRUCT for {}'.format(root.split('/')[-2]))
                    no_rt.append(sub_name)
    return list(set(data))


def check_rts(rts, regex):

    ds = pd.read_file(rts)
    reg_expression = re.compile(regex)
    matching_regex = False
    for i in range(len(ds.StructureSetROISequence)):
        match = reg_expression.match(ds.StructureSetROISequence[i].ROIName)
        if match is not None:
            matching_regex = True
            break
    return matching_regex


def get_files(url, location, file, ext='.tar.gz'):

    if not os.path.isfile(os.path.join(location, file+ext)):
        if not os.path.isdir(os.path.join(location)):
            os.makedirs(os.path.join(location))
        r = requests.get(url)
        with open(os.path.join(location, file+ext), 'wb') as f:
            f.write(r.content)
        print(r.status_code)
        print(r.headers['content-type'])
        print(r.encoding)

    return os.path.join(location, file+ext)


def untar(fname):

    untar_dir = os.path.split(fname)[0]
    if fname.endswith("tar.gz"):
        tar = tarfile.open(fname)
        tar.extractall(path=untar_dir)
        tar.close()
        print("Extracted in Current Directory")
    else:
        print("Not a tar.gz file: {}".format(fname))
