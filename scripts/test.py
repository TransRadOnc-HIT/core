from core.database.base import BaseDatabase


indir = '/media/fsforazz/Samsung_T5/pycurt_test_out/workflows_output/Sorted_Data/'
sub_id = 'hh-0001978972'
wd = '/media/fsforazz/Samsung_T5/pycurt_test_out/workflows_output/'

db = BaseDatabase(sub_id, indir, wd)
db.database()
ds = db.create_datasource()
res = ds.run()

print('Done!')