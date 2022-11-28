from dataclasses import dataclass
from pollination_dsl.dag import Inputs, GroupedDAG, task, Outputs
from pollination.honeybee_radiance.grid import MergeFolderData
from pollination.honeybee_radiance.post_process import AnnualIrradianceMetrics
from pollination.path.copy import CopyFile, CopyFileMultiple

# input/output alias
from pollination.alias.inputs.wea import wea_input


@dataclass
class AnnualIrradiancePostprocess(GroupedDAG):
    """Post-process for annual irradiance."""

    # inputs
    input_folder = Inputs.folder(
        description='Folder with DGP results before redistributing the '
        'results to the original grids.'
    )

    grids_info = Inputs.file(
        description='Grids information from the original model.'
    )

    sun_up_hours = Inputs.file(
        description='Sun up hours up file.'
    )

    timestep = Inputs.int(
        description='Input wea timestep. This value will be used to compute '
        'cumulative radiation results.', default=1,
        spec={'type': 'integer', 'minimum': 1, 'maximum': 60}
    )

    wea = Inputs.file(
        description='Wea file.',
        extensions=['wea'],
        alias=wea_input
    )

    @task(template=CopyFileMultiple)
    def copy_sun_up_hours(self, src=sun_up_hours):
        return [
            {
                'from': CopyFileMultiple()._outputs.dst_1,
                'to': 'results/total/sun-up-hours.txt'
            },
            {
                'from': CopyFileMultiple()._outputs.dst_2,
                'to': 'results/direct/sun-up-hours.txt'
            }
        ]

    @task(template=CopyFileMultiple)
    def copy_grid_info(self, src=grids_info):
        return [
            {
                'from': CopyFileMultiple()._outputs.dst_1,
                'to': 'results/total/grids_info.json'
            },
            {
                'from': CopyFileMultiple()._outputs.dst_2,
                'to': 'results/direct/grids_info.json'
            }
        ]

    @task(
        template=MergeFolderData, needs=[copy_sun_up_hours, copy_grid_info],
        sub_paths={'input_folder': 'final/total'}
    )
    def restructure_total_results(
        self, input_folder=input_folder,
        extension='ill'
    ):
        return [
            {
                'from': MergeFolderData()._outputs.output_folder,
                'to': 'results/total'
            }
        ]

    @task(
        template=MergeFolderData, needs=[copy_sun_up_hours, copy_grid_info],
        sub_paths={'input_folder': 'final/direct'}
    )
    def restructure_direct_results(
        self, input_folder=input_folder,
        extension='ill'
    ):
        return [
            {
                'from': MergeFolderData()._outputs.output_folder,
                'to': 'results/direct'
            }
        ]

    @task(
        template=AnnualIrradianceMetrics,
        needs=[restructure_total_results]
    )
    def calculate_metrics(
        self, folder='results/total', wea=wea, timestep=timestep
    ):
        return [
            {
                'from': AnnualIrradianceMetrics()._outputs.metrics,
                'to': 'metrics'
            },
            {
                'from': AnnualIrradianceMetrics()._outputs.timestep_file,
                'to': 'results/total/timestep.txt'
            }
        ]

    @task(template=CopyFile, needs=[calculate_metrics])
    def copy_timestep_file(self, src=calculate_metrics._outputs.timestep_file):
        return [
            {
                'from': CopyFile()._outputs.dst,
                'to': 'results/direct/timestep.txt'
            }
        ]

    results = Outputs.folder(
        source='results', description='results folder.'
    )

    metrics = Outputs.folder(
        source='metrics', description='metrics folder.'
    )
