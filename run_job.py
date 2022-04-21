# run.py
from utkast_mapReduce import KMeansJob

# python3 run_job.py < python_preprocess.csv > file

# Kopier det inn i en fil
# les av den
# Pass navnet til files
# --file



# --table

# while 1 < 2:
for i in range(2):
    if i == 0:
        old_dim = [[41.775185697, -87.659244248],[41.926404101, -87.792881805],[41.846664648, -87.617318718],[41.954345702, -87.726412567]]
    mr_job = KMeansJob(args=['-r', 'hadoop', '--jobconf', 'my.job.settings.starting_values='+ str(old_dim)])
    with mr_job.make_runner() as runner:
        # '--conf-path', 'mrjob.conf',
        new_dim = [0, 0, 0, 0]
        runner.run()
        for key, value in mr_job.parse_output(runner.cat_output()):
            new_dim[key] = value
        if new_dim == old_dim:
            print("DONE")
            print(new_dim)
            break
        old_dim = new_dim
        print(new_dim)

    # you can read external files in the mapper

    # ... etc