"Abstract class used to build any subsequent workflow"
import shutil
import os
import glob
from copy import deepcopy as dc
from core.database.base import BaseDatabase


class BaseWorkflow(BaseDatabase):

    def datasource(self, create_database=True, dict_sequences=None,
                   check_dependencies=False, **kwargs):

        if create_database is True:
            self.database()
        elif not create_database and dict_sequences is not None:
            self.dict_sequences = dict_sequences
        if check_dependencies:
            dict_sequences = self.create_input_specs(**kwargs)

        field_template, template_args, outfields = self.define_datasource_inputs(
            dict_sequences=dict_sequences)
        self.field_template = field_template
        self.template_args = template_args
        self.outfields= outfields

        self.data_source = self.create_datasource()

    @staticmethod
    def find_reference(scans, references):

        for pr in references:
            for scan in scans:
                if pr in scan.split('_')[0]:
                    return scan

    def create_input_specs(self, **kwargs):

        dict_sequences = dc(self.dict_sequences)
        dict_sequences_updated = {}
        dict_sequences_updated['RT'] = dict_sequences['RT']
        dict_sequences_updated['OT'] = {}
        dict_sequences_updated['MR-RT'] = {}
        sessions = {**dict_sequences['MR-RT'], **dict_sequences['OT']}
        try:
            additional_inputs = self.additional_inputs
        except AttributeError:
            additional_inputs = None
        workflow_input_specs = self.workflow_inputspecs(additional_inputs)
        workflow_inputs = workflow_input_specs['inputs']
        additional_scans = workflow_input_specs['additional_inputs']
        scans_not_found = {}
        scans_not_found['RT'] = dict_sequences['RT']
        for input_key in workflow_inputs:
            process_dependency = False
            in_possible_sequences = workflow_inputs[input_key]['possible_sequences']
            input_multiplicity = workflow_inputs[input_key]['multiplicity']
            input_format = workflow_inputs[input_key]['format']
            input_dependency = workflow_inputs[input_key]['dependency']
            composite_input = workflow_inputs[input_key]['composite']
            mandatory = workflow_inputs[input_key]['mandatory']
            for session_type in ['MR-RT', 'OT']:
                sessions = dict_sequences[session_type]
                scans_not_found[session_type] = {}
                for session in sessions:
                    original_scans = [x.split('_')[0] for x in sessions[session]['scans']]
                    if in_possible_sequences:
                        if input_multiplicity == 'all':
                            scans = [x for x in original_scans if x in in_possible_sequences]
                        elif (input_multiplicity == 'mrrt' and self.dict_sequences['MR-RT']
                                and session_type != 'MR-RT'):
                            scans = [self.find_reference(original_scans, in_possible_sequences)]
                            if not scans_not_found['MR-RT']:
                                scans_not_found['MR-RT'] = dc(
                                    self.dict_sequences['MR-RT'])
                        elif (input_multiplicity == 'rt' and session_type == 'MR-RT'
                                and self.dict_sequences['RT']):
                            scans = [self.find_reference(original_scans, in_possible_sequences)]
                        else:
                            scans = []
                        scans = [x+input_key for x in scans]
                    elif input_key:
                        scans = [input_key]
                    else:
                        scans = original_scans
                    not_found = [x for x in scans if not os.path.isfile(os.path.join(
                            self.input_dir, self.sub_id, session, x+input_format))]
                    if not_found and composite_input is not None:
                        mandatory_scan_type = [x.split('_')[0] for x in composite_input]
                        intersection = [x for x in original_scans if x in mandatory_scan_type]
                        if len(intersection) == len(composite_input):
                            not_found = composite_input
                        else:
                            not_found = []
                            scans.remove(input_key)
                    if not_found and mandatory:
                        process_dependency = True
                        if session not in scans_not_found[session_type].keys():
                            scans_not_found[session_type][session] = {}
                            scans_not_found[session_type][session]['scans'] = not_found
                        else:
                            old_scans = scans_not_found[session_type][session]['scans']
                            new_scans = list(set().union(old_scans, not_found))
                            scans_not_found[session_type][session]['scans'] = new_scans
                    elif not_found and not mandatory:
                        for el in not_found:
                            scans.remove(el)
                    if additional_scans is not None:
                        add_scans_found = [x for x in additional_scans if os.path.isfile(os.path.join(
                            self.input_dir, self.sub_id, session, x+'.nii.gz'))]
                        scans = scans + add_scans_found
                    if session not in dict_sequences_updated[session_type].keys():
                        dict_sequences_updated[session_type][session] = {}
                        dict_sequences_updated[session_type][session]['scans'] = scans
                    else:
                        old_scans = dict_sequences_updated[session_type][session]['scans']
                        new_scans = list(set(old_scans + scans))
                        dict_sequences_updated[session_type][session]['scans'] = new_scans
            if process_dependency:
                torun = input_dependency(**self.__dict__)
                wf = torun.workflow_setup(create_database=False,
                                          dict_sequences=dc(scans_not_found),
                                          check_dependencies=True)
                self.run_dependencies(torun, wf)

        self.dict_sequences = dict_sequences_updated
        return dict_sequences_updated 

    @staticmethod
    def workflow_inputspecs():

        input_specs = {}
        input_specs['format'] = ''
        input_specs['dependencies'] = []
        input_specs['suffix'] = ['']
        input_specs['prefix'] = []

        return input_specs

    @staticmethod
    def workflow_outputspecs():

        output_specs = {}
        output_specs['format'] = ''
        output_specs['suffix'] = []
        output_specs['prefix'] = []

        return output_specs

    def workflow(self):
        raise NotImplementedError

    def create_dependency_workflow(self, dependency_config, index, **kwargs):

        dependency = dependency_config[index]['workflow']
        sessions = dependency_config[index]['scans']
        torun = dependency(**kwargs)
        wf = torun.workflow_setup(create_database=False,
                                  dict_sequences=sessions,
                                  check_dependencies=True)
        return torun, wf

    def run_dependencies(self, torun, wf):

        torun.runner(wf)
        tocopy = sorted(glob.glob(os.path.join(
            self.work_dir, 'workflows_output', torun.workflow_name,
            self.sub_id, '*', '*{}'.format(self.extention))))
        tocopy = [x for x in tocopy if os.path.isfile(x)]
        for tc in tocopy:
            session_name, scan = tc.split('/')[-2:]
            shutil.copy2(tc, os.path.join(self.input_dir, self.sub_id,
                                          session_name, scan))
        
    def workflow_setup(self, create_database=True, dict_sequences=None,
                       check_dependencies=False, **kwargs):
        if kwargs:
            self.__dict__.update(kwargs)
        self.datasource(create_database=create_database,
                        dict_sequences=dict_sequences,
                        check_dependencies=check_dependencies)
        return self.workflow()

    def runner(self, workflow):

        if self.cores == 0 or 'segmentation' in self.workflow_name.lower():
            print('Workflow will run linearly')
            workflow.run()
        else:
            print('Workflow will run in parallel using {} cores'.format(self.cores))
            workflow.run(plugin='MultiProc', plugin_args={'n_procs' : self.cores})

        if self.local_sink:
            self.local_datasink()
