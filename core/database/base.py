"Abstract class used to create any database"
import os
import glob
import nipype
from core.utils.utils import check_dcm_dose
from core.utils.filemanip import split_filename
from core.database.local import LocalDatabase


class BaseDatabase():

    def __init__(self, sub_id, input_dir, work_dir, process_rt=False,
                 local_sink=True, local_project_id=None,
                 local_basedir='', normilize_mr_rt=False,
                 normilize_rtct=False, **kwargs):

        self.sub_id = sub_id
        self.input_dir = input_dir
        self.process_rt = process_rt
        self.work_dir = work_dir
        self.normilize_mr_rt = normilize_mr_rt
        self.normilize_rtct = normilize_rtct
        self.nipype_cache = os.path.join(work_dir, 'nipype_cache', sub_id)
        self.result_dir = os.path.join(work_dir, 'workflows_output')
        self.workflow_name = self.__class__.__name__
        self.outdir = os.path.join(self.result_dir, self.workflow_name)
        self.input_needed = []
        self.local_sink = local_sink
        self.input_specs = {}
        self.output_specs = {}
        if local_project_id is None:
            local_project_id = 'Radiants_database'
        if not local_basedir:
            local_basedir = work_dir
        self.local_project_id = local_project_id
        self.local_basedir = local_basedir
        if kwargs:
            self.__dict__.update(kwargs)
        if local_sink:
            self.local = LocalDatabase(
                project_id=self.local_project_id, 
                local_basedir=self.local_basedir)
    
    def database(self):
        
        base_dir = self.input_dir
        sub_id = self.sub_id

        dict_sequences = {}
        dict_sequences['MR-RT'] = {}
        dict_sequences['RT'] = {}
        dict_sequences['OT'] = {}

        mr_rt_session = [x for x in os.listdir(os.path.join(base_dir, sub_id))
                         if 'MR-RT' in x and os.path.isdir(
                             os.path.join(base_dir, sub_id, x))]
        try:
            self.extention = self.workflow_inputspecs()['format']
        except:
            self.extention = '.nii.gz'
        if mr_rt_session:
            mrs = mr_rt_session[0]
            dict_sequences['MR-RT'][mrs] = {}

            ot = list(set([split_filename(x)[1].split('_')[0]
                           for x in os.listdir(os.path.join(
                               base_dir, sub_id, mrs))]))
            if ot:
                dict_sequences['MR-RT'][mrs]['scans'] = ot
            else:
                dict_sequences['MR-RT'][mrs]['scans'] = None
        
        ot_sessions = [x for x in os.listdir(os.path.join(base_dir, sub_id))
                       if 'MR-RT' not in x and '_RT' not in x 
                       and os.path.isdir(os.path.join(base_dir, sub_id, x))]
        if ot_sessions:
            for session in ot_sessions:
                dict_sequences['OT'][session] = {}
                ot = list(set([split_filename(x)[1].split('_')[0]
                      for x in os.listdir(os.path.join(base_dir, sub_id, session))
                      if x != 'CT']))
                if ot:
                    dict_sequences['OT'][session]['scans'] = ot
                else:
                    dict_sequences['OT'][session]['scans'] = None

        rt_sessions = sorted([x for x in os.listdir(os.path.join(base_dir, sub_id))
                              if '_RT' in x and os.path.isdir(
                                  os.path.join(base_dir, sub_id, x))])
        if rt_sessions:
            for session in rt_sessions:
                dict_sequences['RT'][session] = {}
                dict_sequences['RT'][session]['phy_dose'] = None
                dict_sequences['RT'][session]['rbe_dose'] = None
                dict_sequences['RT'][session]['ot_dose']  = None
                dict_sequences['RT'][session]['rtct'] = None
                dict_sequences['RT'][session]['rtstruct'] = None

                try:
                    physical = [x for x in os.listdir(os.path.join(
                                    base_dir, sub_id, session, 'RTDOSE'))
                                if '1-PHY' in x]
                except FileNotFoundError:
                    physical = []
                    pass
                if physical:
                    dcms = [x for y in physical for x in glob.glob(os.path.join(
                            base_dir, sub_id, session, 'RTDOSE', y, '*.dcm'))]
                    right_dcm = check_dcm_dose(dcms)
                    if right_dcm:
                        dict_sequences['RT'][session]['phy_dose'] = 'RTDOSE/1-PHY*'
                elif not physical and self.extention:
                    physical = [x for x in os.listdir(os.path.join(
                        base_dir, sub_id, session)) if '1-PHY' in x]
                    if physical:
                        dict_sequences['RT'][session]['phy_dose'] = physical[0]
                try:
                    rbe = [x for x in os.listdir(os.path.join(
                        base_dir, sub_id, session, 'RTDOSE')) if '1-RBE' in x]
                except FileNotFoundError:
                    rbe = []
                    pass
                if rbe:
                    dcms = [x for y in rbe for x in glob.glob(os.path.join(
                            base_dir, sub_id, session, 'RTDOSE', y, '*.dcm'))]
                    right_dcm = check_dcm_dose(dcms)
                    if right_dcm:
                        dict_sequences['RT'][session]['rbe_dose'] = 'RTDOSE/1-RBE*'
                elif not rbe and self.extention:
                    rbe = [x for x in os.listdir(os.path.join(
                        base_dir, sub_id, session)) if '1-RBE' in x]
                    if rbe:
                        dict_sequences['RT'][session]['rbe_dose'] = rbe[0]
                try:
                    ot_dose = [x for x in os.listdir(os.path.join(
                        base_dir, sub_id, session, 'RTDOSE')) if '1-RBE' not in x
                        and '1-PHY' not in x]
                except FileNotFoundError:
                    ot_dose = []
                    pass
                if ot_dose:
                    dcms = [x for y in ot_dose for x in glob.glob(os.path.join(
                            base_dir, sub_id, session, 'RTDOSE', y, '*.dcm'))]
                    right_dcm = check_dcm_dose(dcms)
                    if right_dcm:
                        dict_sequences['RT'][session]['ot_dose'] = [
                            'RTDOSE/'+x for x in ot_dose]
                elif not ot_dose and self.extention:
                    ot_dose = [x for x in os.listdir(os.path.join(
                        base_dir, sub_id, session)) if '1-RBE' not in x
                        and '1-PHY' not in x and 'DOSE' in x]
                    if ot_dose:
                        dict_sequences['RT'][session]['ot_dose'] = ot_dose
                if os.path.isdir(os.path.join(base_dir, sub_id, session, 'RTSTRUCT')):
                    rtstruct = [x for x in os.listdir(os.path.join(
                        base_dir, sub_id, session, 'RTSTRUCT')) if '1-' in x]
                    if rtstruct:
                        dict_sequences['RT'][session]['rtstruct'] = 'RTSTRUCT/1-*'
                else:
                    if os.path.isdir(os.path.join(
                            base_dir, sub_id, session, 'RTSTRUCT_used')):
                        dict_sequences['RT'][session]['rtstruct'] = 'RTSTRUCT_used/*.dcm'
                if os.path.isdir(os.path.join(base_dir, sub_id, session, 'RTCT')):
                    rtct = [x for x in os.listdir(os.path.join(
                        base_dir, sub_id, session, 'RTCT')) if '1-' in x]
                    if rtct:
                        dict_sequences['RT'][session]['rtct'] = 'RTCT/1-*'
                elif self.extention:
                    rtct = [x for x in os.listdir(os.path.join(
                        base_dir, sub_id, session)) if 'RTCT.nii.gz' in x]
                    if rtct:
                        dict_sequences['RT'][session]['rtct'] = rtct[0]
    

        self.dict_sequences = dict_sequences

    def define_datasource_inputs(self, dict_sequences=None):

        if dict_sequences is None:
            dict_sequences = self.dict_sequences
        else:
            self.dict_sequences = dict_sequences

        outfields = []
        field_template = dict()
        template_args = dict()

        if not self.extention:
            field_template_string = '%s/{0}/{1}/1-*'
        else:
            field_template_string = '%s/{0}/{1}'

        field_template_rt_string = '%s/{0}/{1}'

        try:
            data_formats = self.workflow_inputspecs()['data_formats']
        except:
            data_formats = {'':'.nii.gz'}

        for key in dict_sequences['MR-RT']:
            if dict_sequences['MR-RT'][key]['scans'] is not None:
                for el in dict_sequences['MR-RT'][key]['scans']:
                    field_name = '{0}_{1}'.format(key, el)
                    contrast = el.split('_')[0]
                    extention = None
                    for k in data_formats:
                        if '{0}{1}'.format(contrast, k) == el:
                            extention = data_formats[k]
                    if extention == None:
                        for k in data_formats:
                            if k == el:
                                extention = data_formats[k]
                    if extention is None:
                        extention = '.nii.gz'
                    outfields.append(field_name)
                    field_template[field_name] = field_template_string.format(
                        key, el+extention)
                    template_args[field_name] = [['sub_id']]
        for key in dict_sequences['OT']:
            if dict_sequences['OT'][key]['scans'] is not None:
                for el in dict_sequences['OT'][key]['scans']:
                    field_name = '{0}_{1}'.format(key, el)#.strip(self.extention))
                    contrast = el.split('_')[0]
                    extention = None
                    for k in data_formats:
                        if '{0}{1}'.format(contrast, k) == el:
                            extention = data_formats[k]
                    if extention == None:
                        for k in data_formats:
                            if k == el:
                                extention = data_formats[k]
                    if extention is None:
                        extention = '.nii.gz'
                    outfields.append(field_name)
                    field_template[field_name] = field_template_string.format(
                        key, el+extention)
                    template_args[field_name] = [['sub_id']]
        for key in dict_sequences['RT']:
            if dict_sequences['RT'][key]['phy_dose'] is not None:
                field_name = '{}_phy_dose'.format(key)
                outfields.append(field_name)
                field_template[field_name] = field_template_rt_string.format(
                    key, dict_sequences['RT'][key]['phy_dose'])
                template_args[field_name] = [['sub_id']]
            if dict_sequences['RT'][key]['rbe_dose'] is not None:
                field_name = '{}_rbe_dose'.format(key)
                outfields.append(field_name)
                field_template[field_name] = field_template_rt_string.format(
                    key, dict_sequences['RT'][key]['rbe_dose'])
                template_args[field_name] = [['sub_id']]
            if dict_sequences['RT'][key]['rtct'] is not None:
                field_name = '{}_rtct'.format(key)
                outfields.append(field_name)
                field_template[field_name] = field_template_rt_string.format(
                    key, dict_sequences['RT'][key]['rtct'])
                template_args[field_name] = [['sub_id']]
            if dict_sequences['RT'][key]['rtstruct'] is not None:
                field_name = '{}_rtstruct'.format(key)
                outfields.append(field_name)
                field_template[field_name] = field_template_rt_string.format(
                    key, dict_sequences['RT'][key]['rtstruct'])
                template_args[field_name] = [['sub_id']]
            if dict_sequences['RT'][key]['ot_dose'] is not None:
                field_name = '{}_ot_dose'.format(key)
                outfields.append(field_name)
                field_template[field_name] = field_template_rt_string.format(
                    key, dict_sequences['RT'][key]['ot_dose'][0])
                template_args[field_name] = [['sub_id']]
    
        return field_template, template_args, outfields

    def create_datasource(self):
        
        datasource = nipype.Node(
            interface=nipype.DataGrabber(
                infields=['sub_id'],
                outfields=self.outfields),
                name='datasource')
        datasource.inputs.base_directory = self.input_dir
        datasource.inputs.template = '*'
        datasource.inputs.sort_filelist = True
        datasource.inputs.raise_on_empty = False
        datasource.inputs.field_template = self.field_template
        datasource.inputs.template_args = self.template_args
        datasource.inputs.sub_id = self.sub_id
        
        return datasource

    def local_datasink(self):

        sub_folder = os.path.join(self.outdir, self.sub_id)
        if os.path.isdir(sub_folder):
            sessions = [x for x in sorted(os.listdir(sub_folder))
                        if os.path.isdir(os.path.join(sub_folder, x))]
            
            self.local.put(sessions, sub_folder)
        else:
            print('Nothing to copy for subject {}'.format(self.sub_id))
