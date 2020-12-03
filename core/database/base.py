import os
import glob
import nipype
from nipype.interfaces.utility import Split
from core.utils.utils import check_dcm_dose, string_strip
from core.utils.filemanip import split_filename
from builtins import FileNotFoundError
# from pycurt.database.local import LocalDatabase
# from pycurt.database.utils import check_cache


POSSIBLE_SEQUENCES = ['t1', 'ct1', 't1km', 't2', 'flair', 'adc', 'swi', 'rtct',
                      'rtdose', 'rtplan', 'rtstruct']
POSSIBLE_REFERENCES = ['T1c', 'T1KM', 'T1']


class BaseDatabase():

    def __init__(self, sub_id, input_dir, work_dir, process_rt=False,
                 local_source=False, local_sink=False, local_project_id=None,
                 local_basedir=''):

        self.sub_id = sub_id
        self.base_dir = input_dir
        self.process_rt = process_rt
        self.nipype_cache = os.path.join(work_dir, 'nipype_cache', sub_id)
        self.result_dir = os.path.join(work_dir, 'workflows_output')
        self.workflow_name = self.__class__.__name__
        self.outdir = os.path.join(self.result_dir, self.workflow_name)
        self.input_needed = []
        self.local_sink = local_sink
        self.local_source = local_source
#         if local_source or local_sink:
#             self.local = LocalDatabase(
#                 project_id=local_project_id, 
#                 local_basedir=local_basedir)
    
    def database(self):
        
        base_dir = self.base_dir
        sub_id = self.sub_id

        dict_sequences = {}
        dict_sequences['MR-RT'] = {}
        dict_sequences['RT'] = {}
        dict_sequences['OT'] = {}

        mr_rt_session = [x for x in os.listdir(os.path.join(base_dir, sub_id))
                         if 'MR-RT' in x and os.path.isdir(
                             os.path.join(base_dir, sub_id, x))]
        if mr_rt_session:
            mrs = mr_rt_session[0]
            dict_sequences['MR-RT'][mrs] = {}
            ref = [x for x in os.listdir(os.path.join(base_dir, sub_id, mrs))
                   if split_filename(x)[1] in POSSIBLE_REFERENCES]
            extentions = [split_filename(x)[2] for x in
                          os.listdir(os.path.join(base_dir, sub_id, mrs))]
            if len(set(extentions)) > 1:
                raise Exception('{} different file extentions were found. In order '
                                'to use  this tool only one common extention '
                                'can be used.'.format(len(set(extentions))))
            else:
                self.extention = extentions[0]
            ref = [x for y in POSSIBLE_REFERENCES for x in ref
                   if split_filename(x)[1] == y]
            if ref:
                dict_sequences['MR-RT'][mrs]['ref'] = ref[0]
                ot = [x for x in os.listdir(os.path.join(base_dir, sub_id, mrs))
                      if x != ref[0]]
            else:
                dict_sequences['MR-RT'][mrs]['ref'] = None
                ot = [x for x in os.listdir(os.path.join(base_dir, sub_id, mrs))]
            if ot:
                dict_sequences['MR-RT'][mrs]['other'] = ot
            else:
                dict_sequences['MR-RT'][mrs]['other'] = None
        
        ot_sessions = [x for x in os.listdir(os.path.join(base_dir, sub_id))
                       if 'MR-RT' not in x and '_RT' not in x 
                       and os.path.isdir(os.path.join(base_dir, sub_id, x))]
        if ot_sessions:
            for session in ot_sessions:
                dict_sequences['OT'][session] = {}
                ref = [x for x in os.listdir(os.path.join(base_dir, sub_id, session))
                       if split_filename(x)[1] in POSSIBLE_REFERENCES]
                ref = [x for y in POSSIBLE_REFERENCES for x in ref
                       if split_filename(x)[1] == y]
                if ref:
                    dict_sequences['OT'][session]['ref'] = ref[0]
                    ot = [x for x in os.listdir(os.path.join(base_dir, sub_id, session))
                          if x != ref[0]]
                else:
                    dict_sequences['OT'][session]['ref'] = None
                    ot = [x for x in os.listdir(os.path.join(base_dir, sub_id, session))]
                if ot:
                    dict_sequences['OT'][session]['other'] = ot
                else:
                    dict_sequences['OT'][session]['other'] = None

        rt_sessions = [x for x in os.listdir(os.path.join(base_dir, sub_id))
                      if '_RT' in x and os.path.isdir(
                          os.path.join(base_dir, sub_id, x))]
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
                    dcms = ['RTDOSE/'+x for y in ot_dose for x in glob.glob(os.path.join(
                            base_dir, sub_id, session, 'RTDOSE', y, '*.dcm'))]
                    right_dcm = check_dcm_dose(dcms)
                    if right_dcm:
                        dict_sequences['RT'][session]['ot_dose'] = ot_dose
                elif not ot_dose and self.extention:
                    ot_dose = [x for x in os.listdir(os.path.join(
                        base_dir, sub_id, session)) if '1-RBE' not in x
                        and '1-PHY' not in x]
                    if ot_dose:
                        dict_sequences['RT'][session]['ot_dose'] = ot_dose
                if os.path.isdir(os.path.join(base_dir, sub_id, session, 'RTSTRUCT')):
                    rtstruct = [x for x in os.listdir(os.path.join(
                        base_dir, sub_id, session, 'RTSTRUCT')) if '1-' in x]
                    if rtstruct:
                        dict_sequences['RT'][session]['rtstruct'] = 'RTSTRUCT/1-*'
                if os.path.isdir(os.path.join(base_dir, sub_id, session, 'RTCT')):
                    rtct = [x for x in os.listdir(os.path.join(
                        base_dir, sub_id, session, 'RTCT')) if '1-' in x]
                    if rtct:
                        dict_sequences['RT'][session]['rtct'] = 'RTCT/1-*'
                    elif not rtct and self.extention:
                        rtct = [x for x in os.listdir(os.path.join(
                            base_dir, sub_id, session)) if 'RTCT.nii.gz' in x]
                        if rtct:
                            dict_sequences['RT'][session]['rtct'] = rtct[0]
    

        self.dict_sequences = dict_sequences

        field_template, template_args, outfields = self.define_datasource_inputs()

        self.field_template = field_template
        self.template_args = template_args
        self.outfields= outfields

    def define_datasource_inputs(self):

        dict_sequences = self.dict_sequences
        outfields = []
        field_template = dict()
        template_args = dict()

        if not self.extention:
            field_template_string = '%s/{0}/{1}/1-*'
        else:
            field_template_string = '%s/{0}/{1}'

        field_template_rt_string = '%s/{0}/{1}'

        for key in dict_sequences['MR-RT']:
            if dict_sequences['MR-RT'][key]['ref'] is not None:
                field_name = '{}_ref'.format(key)
                outfields.append(field_name)
                field_template[field_name] = field_template_string.format(
                    key, dict_sequences['MR-RT'][key]['ref'])
                template_args[field_name] = [['sub_id']]
            if dict_sequences['MR-RT'][key]['other'] is not None:
                for el in dict_sequences['MR-RT'][key]['other']:
                    field_name = '{0}_{1}'.format(key, el)
                    outfields.append(field_name)
                    field_template[field_name] = field_template_string.format(
                        key, el)
                    template_args[field_name] = [['sub_id']]
        for key in dict_sequences['OT']:
            if dict_sequences['OT'][key]['ref'] is not None:
                field_name = '{}_ref'.format(key)
                outfields.append(field_name)
                field_template[field_name] = field_template_string.format(
                    key, dict_sequences['OT'][key]['ref'])
                template_args[field_name] = [['sub_id']]
            if dict_sequences['OT'][key]['other'] is not None:
                for el in dict_sequences['OT'][key]['other']:
                    field_name = '{0}_{1}'.format(key, el)
                    outfields.append(field_name)
                    field_template[field_name] = field_template_string.format(
                        key, el)
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
                for el in dict_sequences['RT'][key]['ot_dose']:
                    field_name = '{0}_{1}'.format(key, el)
                    outfields.append(field_name)
                    field_template[field_name] = field_template_rt_string.format(
                        key, el)
                    template_args[field_name] = [['sub_id']]
    
        return field_template, template_args, outfields

    def create_datasource(self):
        
        datasource = nipype.Node(
            interface=nipype.DataGrabber(
                infields=['sub_id'],
                outfields=self.outfields),
                name='datasource')
        datasource.inputs.base_directory = self.base_dir
        datasource.inputs.template = '*'
        datasource.inputs.sort_filelist = True
        datasource.inputs.raise_on_empty = False
        datasource.inputs.field_template = self.field_template
        datasource.inputs.template_args = self.template_args
        datasource.inputs.sub_id = self.sub_id
        
        return datasource

    def datasink(self, workflow, workflow_datasink):

        datasource = self.data_source
        sequences1 = [x for x in datasource.inputs.field_template.keys()
                      if x!='t1_0' and x!='reference' and x!='rt' and x!='rt_dose'
                      and x!='doses' and x!='rts_dcm' and x!='rtstruct'
                      and x!='physical' and x!='rbe' and x!='rtct' and x!='rtct_nifti']
        rt = [x for x in datasource.inputs.field_template.keys()
              if x=='rt']
    
        split_ds_nodes = []
        for i in range(len(sequences1)):
            sessions_wit_seq = [
                x for y in self.sessions for x in glob.glob(os.path.join(
                    self.base_dir, self.sub_id, y, sequences1[i].upper()+'.nii.gz'))]
            split_ds = nipype.Node(interface=Split(), name='split_ds{}'.format(i))
            split_ds.inputs.splits = [1]*len(sessions_wit_seq)
            split_ds_nodes.append(split_ds)

            if len(sessions_wit_seq) > 1:
                workflow.connect(datasource, sequences1[i], split_ds,
                                 'inlist')
                for j, sess in enumerate(sessions_wit_seq):
                    sess_name = sess.split('/')[-2]
                    workflow.connect(split_ds, 'out{}'.format(j+1),
                                     workflow_datasink, 'results.subid.{0}.@{1}'
                                     .format(sess_name, sequences1[i]))
            elif len(sessions_wit_seq) == 1:
                workflow.connect(datasource, sequences1[i], workflow_datasink,
                                 'results.subid.{0}.@{1}'
                                 .format(sessions_wit_seq[0].split('/')[-2],
                                         sequences1[i]))
        if self.reference:
            workflow.connect(datasource, 'reference', workflow_datasink,
                             'results.subid.REF.@ref_ct')
        if self.t10:
            workflow.connect(datasource, 't1_0', workflow_datasink,
                             'results.subid.T10.@ref_t1')
        if rt:
            workflow.connect(datasource, 'rt', workflow_datasink,
                             'results.subid.@rt')
        return workflow
    
    def local_datasource(self):
        
        skip_sessions = check_cache(self.sessions, self.input_needed,
                                    self.sub_id, self.base_dir)
        
        if [x for x in self.sessions if x not in skip_sessions]:
            self.local.get(self.base_dir, subjects=[self.sub_id],
                             needed_scans=self.input_needed,
                             skip_sessions=skip_sessions)

    def local_datasink(self):

        sub_folder = os.path.join(self.outdir, self.sub_id)
        if os.path.isdir(sub_folder):
            sessions = [x for x in sorted(os.listdir(sub_folder))
                        if os.path.isdir(os.path.join(sub_folder, x))]
            
            self.local.put(sessions, sub_folder)
        else:
            print('Nothing to copy for subject {}'.format(self.sub_id))
