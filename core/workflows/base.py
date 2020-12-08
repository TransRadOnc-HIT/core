"Abstract class used to build any subsequent workflow"
import shutil
import os
import glob
from core.database.base import BaseDatabase


class BaseWorkflow(BaseDatabase):

    def datasource(self, create_database=True, dict_sequences=None,
                   check_dependencies=False, **kwargs):

        if create_database is True:
            self.database()
        if check_dependencies:
            self.create_input_specs(**kwargs)

        field_template, template_args, outfields = self.define_datasource_inputs(
            dict_sequences=dict_sequences)
        self.field_template = field_template
        self.template_args = template_args
        self.outfields= outfields

        self.data_source = self.create_datasource()
        
        if self.local_source:
            self.local_source()

    def create_input_specs(self, **kwargs):

        if kwargs and 'possible_sequences' in kwargs.keys():
            possible_sequences = kwargs['possible_sequences']
        else:
            possible_sequences = []
        dict_sequences = self.dict_sequences
        workflow_input_specs = self.workflow_inputspecs()
        process = False
        run_dependency_config = {}
        n_dependencies = len(workflow_input_specs['dependencies'])-1
        for i, dependency in enumerate(workflow_input_specs['dependencies']):
            run_dependency_config[n_dependencies-i] = {}
            run_dependency_config[n_dependencies-i]['workflow'] = dependency
            sessions = {**dict_sequences['MR-RT'], **dict_sequences['OT']}
            scans_not_found = {}
            scans_not_found['RT'] = dict_sequences['RT']
            dependency_outspecs = dependency.workflow_outputspecs()
            dependency_outputs = dependency_outspecs['suffix']
            dependency_inspecs = dependency.workflow_inputspecs()
            dependency_inputs = dependency_inspecs['suffix']
            for session_type in ['MR-RT', 'OT']:
                scans_not_found[session_type] = {}
                sessions = dict_sequences[session_type]
                for needed_out in dependency_outputs:
                    for key in sessions:
                        scans = [x+needed_out
                                 for x in sessions[key]['scans']]
                        if possible_sequences:
                            scans = [x for x in scans if x in possible_sequences]
                        not_found = []
                        for s in dependency_inputs:
                            not_found = not_found+ [
                                x.split('_')[0]+s for x in scans
                                if not os.path.isfile(os.path.join(
                                    self.base_dir, self.sub_id, key,
                                    x+workflow_input_specs['format']))]
                        if not_found:
                            process = True
                            if key not in scans_not_found[session_type].keys():
                                scans_not_found[session_type][key] = {}
                                scans_not_found[session_type][key]['scans'] = not_found
                            else:
                                old_scans = scans_not_found[session_type][key]['scans']
                                new_scans = list(set().union(old_scans, not_found))
                                scans_not_found[session_type][key]['scans'] = new_scans
            run_dependency_config[n_dependencies-i]['scans'] = scans_not_found
            dict_sequences = scans_not_found.copy()
                
        if process:
            self.run_dependencies(run_dependency_config,
                                  extention=self.extention)

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
    
    def run_dependencies(self, dependency_config, **kwargs):
        
        for i in range(len(dependency_config.keys())):
            dependency = dependency_config[i]['workflow']
            sessions = dependency_config[i]['scans']
            torun = dependency(sub_id=self.sub_id, input_dir=self.base_dir,
                               work_dir=self.work_dir)
            wf = torun.workflow_setup(create_database=False,
                                      dict_sequences=sessions, **kwargs)
            torun.runner(wf)
            tocopy = sorted(glob.glob(os.path.join(
                self.work_dir, 'workflows_output', torun.__class__.__name__,
                self.sub_id, '*', '*{}'.format(self.extention))))
            for tc in tocopy:
                session_name, scan = tc.split('/')[-2:]
                shutil.copy2(tc, os.path.join(self.base_dir, self.sub_id,
                                              session_name, scan))
        
    def workflow_setup(self, create_database=True, dict_sequences=None,
                       **kwargs):
        if kwargs:
            self.__dict__.update(kwargs)
        self.datasource(create_database=create_database,
                        dict_sequences=dict_sequences)
        return self.workflow()

    def runner(self, workflow, cores=0):

        if cores == 0:
            print('Workflow will run linearly')
            workflow.run()
        else:
            print('Workflow will run in parallel using {} cores'.format(cores))
            workflow.run(plugin='MultiProc', plugin_args={'n_procs' : cores})

        if self.local_sink:
            self.local_datasink()
