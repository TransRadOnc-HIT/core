from core.database.base import BaseDatabase
from more_itertools import collapse
from core.utils.filemanip import split_filename


class BaseWorkflow(BaseDatabase):

    def datasource(self):

        self.database()
        self.data_source = self.create_datasource()
        
        if self.local_source:
            self.local_source()

    def create_input_specs(self):

        self.datasource()
        dict_sequences = self.dict_sequences
        sessions = {**dict_sequences['MR-RT'], **dict_sequences['OT'],
                    **dict_sequences['RT']}

        inputs_avail = [sessions[y][x] for y in sessions for x in sessions[y]]
        inputs_avail = list(set(collapse(inputs_avail)))
        formats = list(set([self.input_specs[key]['format']
                            for key in self.input_specs]))
        if len(formats) == 1:
            if formats[0] == 'NIFTI_GZ':
                output_ext = '.nii.gz'
            elif formats[0] == 'DICOM':
                output_ext = ''
        avail_input_formats = [split_filename(x)[-1] for x in inputs_avail]
        if len(set(avail_input_formats)) == 1:
            if list(set(avail_input_formats))[0] == output_ext:
                self.workflow_torun.append(self.__class__)
        if list(set(formats))[0] == self.extention:
            self.ready = True
        else:
            self.ready = False
            self.additional_workflows.append()
             
    def workflow_inputspecs(self):
        raise NotImplementedError

    def workflow_outputspecs(self):
        raise NotImplementedError

    def workflow(self):
        raise NotImplementedError
    
    def workflow_setup(self):
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